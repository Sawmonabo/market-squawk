//! Exact source-reported quote prices without inferred trading increments or execution authority.

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    AssetClass, AssignmentVerification, Currency, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier, ExternalIdentifierRecord,
    IdentifierEntitlement, InstrumentId, LiveEventClass, LiveProvenance,
    MarketDataInstrumentDefinition, Money, ProviderIdentityRecord, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, Timestamp,
};

/// Source field presence and exact value when its economic quantity unit is not established.
///
/// An unresolved value is never a number of shares, contracts, lots or base-currency units.
/// Consumers may retain it as source evidence but cannot use it for depth or execution sizing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "state",
    content = "value",
    rename_all = "snake_case"
)]
pub enum MarketDataQuoteSize {
    /// No source field was supplied.
    Absent,
    /// The source explicitly supplied null.
    Null,
    /// An exact supplied decimal whose unit has not been proven by a source contract.
    UnresolvedUnit(Decimal),
}

/// A currency-qualified quoted price with its source size state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataQuoteSide {
    price: Money,
    size: MarketDataQuoteSize,
}

impl MarketDataQuoteSide {
    /// Retains exact decimal price and size state; relational validation occurs on the event.
    pub const fn new(price: Money, size: MarketDataQuoteSize) -> Self {
        Self { price, size }
    }
    /// Returns the actual monetary price.
    pub const fn price(&self) -> Money {
        self.price
    }
    /// Returns supplied size without asserting an economic unit.
    pub const fn size(&self) -> &MarketDataQuoteSize {
        &self.size
    }
}

/// Original accepted reference assertion; source namespaces are never relabelled as feed identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "record",
    rename_all = "snake_case"
)]
pub enum MarketDataReferenceIdentity {
    /// Exact provider-native identifier retained by the canonical definition.
    Provider(ProviderIdentityRecord),
    /// Verified assigned ticker or OCC contract retained by the canonical definition.
    Assigned(ExternalIdentifierRecord),
}

/// Compact evidence for the canonical definition and distinct provider-reference namespace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataReference {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    definition_digest: EvidenceDigest,
    definition_evidence: RevisionBoundPayloadEvidence,
    effective: EffectiveInterval,
    currency: Currency,
    currency_evidence: ExactPayloadEvidence,
    identity: MarketDataReferenceIdentity,
    source_symbol: ProviderInstrumentId,
}

impl MarketDataReference {
    /// Admits an identity actually retained by the complete reference definition at the supplied
    /// instant. The application additionally retains the exact catalog record through commit.
    pub fn try_new(
        definition: &MarketDataInstrumentDefinition,
        definition_digest: EvidenceDigest,
        provider_identity: &ProviderIdentityRecord,
        at: Timestamp,
    ) -> Result<Self, MarketDataEventError> {
        if definition.provider_identity_at(
            provider_identity.source_id(),
            provider_identity.provider_instrument_id(),
            at,
        ) != Some(provider_identity)
            || provider_identity.instrument_id() != definition.instrument_id()
        {
            return Err(MarketDataEventError::Reference);
        }
        let result = Self {
            instrument_id: definition.instrument_id(),
            asset_class: definition.asset_class(),
            definition_digest,
            definition_evidence: definition.reference_evidence().clone(),
            effective: definition.effective_interval(),
            currency: definition.quote_currency(),
            currency_evidence: definition.quote_currency_evidence().clone(),
            identity: MarketDataReferenceIdentity::Provider(provider_identity.clone()),
            source_symbol: provider_identity.provider_instrument_id().clone(),
        };
        result.validate_at(at)?;
        Ok(result)
    }

    /// Returns the repository-owned stable instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the reference-established instrument family.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }
    /// Returns the exact immutable catalog definition digest.
    pub const fn definition_digest(&self) -> EvidenceDigest {
        self.definition_digest
    }
    /// Returns the original reference-source assertion and namespace.
    pub const fn provider_identity(&self) -> Option<&ProviderIdentityRecord> {
        match &self.identity {
            MarketDataReferenceIdentity::Provider(identity) => Some(identity),
            MarketDataReferenceIdentity::Assigned(_) => None,
        }
    }
    /// Returns the genuine subscription symbol, independently of the reference source namespace.
    pub const fn source_symbol(&self) -> &ProviderInstrumentId {
        &self.source_symbol
    }
    /// Returns the original closed source assertion retained by the canonical definition.
    pub const fn identity(&self) -> &MarketDataReferenceIdentity {
        &self.identity
    }
    /// Copies one verified assignment actually retained by the immutable definition. This does
    /// not turn a Nasdaq/OCC assertion into an Alpaca provider identity.
    pub fn try_from_assigned_identifier(
        definition: &MarketDataInstrumentDefinition,
        definition_digest: EvidenceDigest,
        identifier: &ExternalIdentifierRecord,
        source_symbol: ProviderInstrumentId,
        at: Timestamp,
    ) -> Result<Self, MarketDataEventError> {
        let matches = assigned_symbol_matches(identifier, &source_symbol);
        if !matches || !definition.identifiers().contains(identifier) {
            return Err(MarketDataEventError::Reference);
        }
        let result = Self {
            instrument_id: definition.instrument_id(),
            asset_class: definition.asset_class(),
            definition_digest,
            definition_evidence: definition.reference_evidence().clone(),
            effective: definition.effective_interval(),
            currency: definition.quote_currency(),
            currency_evidence: definition.quote_currency_evidence().clone(),
            identity: MarketDataReferenceIdentity::Assigned(identifier.clone()),
            source_symbol,
        };
        result.validate_at(at)?;
        Ok(result)
    }
    /// Replays the exact assertion against the catalog's complete definition.
    pub fn validate_definition_at(
        &self,
        definition: &MarketDataInstrumentDefinition,
        at: Timestamp,
    ) -> Result<(), MarketDataEventError> {
        let retained = match &self.identity {
            MarketDataReferenceIdentity::Provider(identity) => {
                Self::try_new(definition, self.definition_digest, identity, at)?
            }
            MarketDataReferenceIdentity::Assigned(identifier) => {
                Self::try_from_assigned_identifier(
                    definition,
                    self.definition_digest,
                    identifier,
                    self.source_symbol.clone(),
                    at,
                )?
            }
        };
        if retained != *self {
            return Err(MarketDataEventError::Reference);
        }
        Ok(())
    }
    /// Returns the currency established by source-qualified reference evidence.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Checks the retained assertion clocks and source identity at an observation instant.
    /// Durable publication additionally replays it against the complete immutable catalog record.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), MarketDataEventError> {
        if self.definition_digest.algorithm() != DigestAlgorithm::Sha256
            || self.definition_digest.bytes() == [0; 32]
            || self
                .definition_evidence
                .payload_evidence()
                .content_digest()
                .bytes()
                == [0; 32]
            || self.currency_evidence.content_digest().bytes() == [0; 32]
            || !contains(self.effective, at)
            || match &self.identity {
                MarketDataReferenceIdentity::Provider(identity) => {
                    identity.instrument_id() != self.instrument_id
                        || identity.provider_instrument_id() != &self.source_symbol
                        || !contains(identity.validity(), at)
                        || identity.observed_at() > at
                        || identity
                            .source_timestamp()
                            .is_some_and(|published| published > at)
                        || identity.evidence().content_digest().bytes() == [0; 32]
                }
                MarketDataReferenceIdentity::Assigned(identifier) => {
                    !assigned_symbol_matches(identifier, &self.source_symbol)
                        || identifier.assignment_verification()
                            != AssignmentVerification::VerifiedAssigned
                        || identifier.rights_policy().entitlement()
                            == IdentifierEntitlement::UnknownOrRestricted
                        || identifier.source_evidence().content_digest().bytes() == [0; 32]
                        || !contains(identifier.validity(), at)
                        || identifier.observed_at() > at
                        || identifier
                            .source_timestamp()
                            .is_some_and(|published| published > at)
                }
            }
        {
            return Err(MarketDataEventError::Reference);
        }
        Ok(())
    }
}

/// One genuine source quote with explicit currency and independent canonical reference lineage.
///
/// This type carries no tick size, lot size, trading status, execution eligibility or executable
/// quantity. A crossed or locked snapshot is retained truthfully and can be excluded by a consumer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataQuoteEvent {
    provenance: LiveProvenance,
    reference: MarketDataReference,
    bid: Option<MarketDataQuoteSide>,
    ask: Option<MarketDataQuoteSide>,
}

impl MarketDataQuoteEvent {
    /// Validates source/canonical identity, time, currency and supplied size without inventing an
    /// execution scale or discarding a genuine locked/crossed observation.
    pub fn try_new(
        provenance: LiveProvenance,
        reference: MarketDataReference,
        bid: Option<MarketDataQuoteSide>,
        ask: Option<MarketDataQuoteSide>,
    ) -> Result<Self, MarketDataEventError> {
        if provenance.instrument_id() != Some(reference.instrument_id())
            || provenance.venue_id().is_none()
            || provenance.binding().event_class() != LiveEventClass::Quote
            || provenance.source_identifier().as_str() != reference.source_symbol().as_str()
            || bid.is_none() && ask.is_none()
        {
            return Err(MarketDataEventError::Binding);
        }
        reference.validate_at(provenance.received_at())?;
        for side in bid.iter().chain(ask.iter()) {
            if side.price.currency() != reference.currency() {
                return Err(MarketDataEventError::Currency);
            }
            if matches!(&side.size, MarketDataQuoteSize::UnresolvedUnit(value) if *value < Decimal::ZERO)
            {
                return Err(MarketDataEventError::Size);
            }
        }
        Ok(Self {
            provenance,
            reference,
            bid,
            ask,
        })
    }
    /// Returns exact raw-capture, source-time and registered feed provenance.
    pub const fn provenance(&self) -> &LiveProvenance {
        &self.provenance
    }
    /// Returns distinct reference-source lineage and canonical definition revision.
    pub const fn reference(&self) -> &MarketDataReference {
        &self.reference
    }
    /// Returns the exact bid side when supplied.
    pub const fn bid(&self) -> Option<&MarketDataQuoteSide> {
        self.bid.as_ref()
    }
    /// Returns the exact ask side when supplied.
    pub const fn ask(&self) -> Option<&MarketDataQuoteSide> {
        self.ask.as_ref()
    }
    /// Reports an observed locked or crossed snapshot without changing its source values.
    pub fn is_locked_or_crossed(&self) -> bool {
        self.bid
            .as_ref()
            .zip(self.ask.as_ref())
            .is_some_and(|(bid, ask)| bid.price.amount() >= ask.price.amount())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketDataQuoteEventWire {
    provenance: LiveProvenance,
    reference: MarketDataReference,
    bid: Option<MarketDataQuoteSide>,
    ask: Option<MarketDataQuoteSide>,
}

impl<'de> Deserialize<'de> for MarketDataQuoteEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = MarketDataQuoteEventWire::deserialize(deserializer)?;
        Self::try_new(wire.provenance, wire.reference, wire.bid, wire.ask)
            .map_err(serde::de::Error::custom)
    }
}

/// Invalid identity, source binding, currency or source-reported size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataEventError {
    /// Canonical reference is missing, stale or cross-bound.
    Reference,
    /// Source quote and canonical identity do not agree.
    Binding,
    /// A price carries another currency.
    Currency,
    /// A supplied quote size is negative.
    Size,
}

impl std::fmt::Display for MarketDataEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Reference => "market reference is invalid or stale",
            Self::Binding => "market source and reference binding disagree",
            Self::Currency => "market price currency differs from its reference",
            Self::Size => "market price or quantity is invalid",
        })
    }
}
impl std::error::Error for MarketDataEventError {}

fn contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    at >= interval.starts_at() && interval.ends_at().is_none_or(|end| at < end)
}

fn assigned_symbol_matches(
    identifier: &ExternalIdentifierRecord,
    symbol: &ProviderInstrumentId,
) -> bool {
    match identifier.identifier() {
        ExternalIdentifier::Ticker(ticker) => ticker.as_str() == symbol.as_str(),
        ExternalIdentifier::OccOption(option) => {
            option.to_string().replace(' ', "") == symbol.as_str()
        }
        _ => false,
    }
}
