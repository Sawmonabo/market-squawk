//! Exact point-in-time provider-native identity selected from durable reference evidence.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, MarketDataInstrumentDefinition,
    MetadataRevision, ProviderIdentityKey, Timestamp, VenueMapping,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

/// Immutable provider-native identity selected from one exact durable instrument definition.
///
/// This value is evidence, not a provider-symbol lookup hint. Construction succeeds only when the
/// definition, provider assertion, venue mapping, publication clock, and effective clocks all
/// agree at the explicit selection instant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderNativeInstrumentAttestation {
    provider_key: ProviderIdentityKey,
    instrument_id: InstrumentId,
    venue_mapping: VenueMapping,
    provider_identity_revision: MetadataRevision,
    provider_identity_digest: EvidenceDigest,
    reference_revision: MetadataRevision,
    reference_digest: EvidenceDigest,
    definition_revision_digest: EvidenceDigest,
    definition_published_at: Timestamp,
    identity_observed_at: Timestamp,
    valid_from: Timestamp,
    valid_until: Option<Timestamp>,
    selected_at: Timestamp,
}

/// Complete durable-record facts needed to select a provider-native identity.
#[derive(Clone, Debug)]
pub struct ProviderNativeInstrumentAttestationInput<'a> {
    /// Exact canonical definition retained by the durable instrument generation.
    pub definition: &'a MarketDataInstrumentDefinition,
    /// SHA-256 identity of the exact durable definition revision.
    pub definition_revision_digest: EvidenceDigest,
    /// Publication time of that durable definition revision.
    pub definition_published_at: Timestamp,
    /// Exact provider namespace and provider-native instrument identifier.
    pub provider_key: ProviderIdentityKey,
    /// Exact venue and venue-native symbol expected for this provider route.
    pub venue_mapping: VenueMapping,
    /// Explicit point-in-time selection instant.
    pub selected_at: Timestamp,
}

/// A provider-native identity could not be proven at the requested point in time.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderNativeInstrumentAttestationError {
    /// One required durable content identity was not a nonzero SHA-256 digest.
    #[error("provider-native instrument evidence digest is invalid")]
    InvalidDigest,
    /// The selected definition had not yet been published.
    #[error("provider-native instrument definition is future-dated")]
    FutureDefinition,
    /// No unique accepted provider assertion was effective at the selection instant.
    #[error("provider-native instrument identity is unavailable or conflicted at selection time")]
    ProviderIdentityUnavailable,
    /// The provider assertion belongs to a different canonical instrument.
    #[error("provider-native instrument identity does not match the canonical instrument")]
    InstrumentMismatch,
    /// The exact venue and venue symbol were not present in the selected definition.
    #[error("provider-native instrument venue mapping does not match the canonical definition")]
    VenueMappingMismatch,
    /// Provider identity evidence was not locally observed by the selection instant.
    #[error("provider-native instrument identity observation is future-dated")]
    FutureObservation,
    /// The canonical definition or provider assertion was not effective at selection time.
    #[error("provider-native instrument identity is outside its validity interval")]
    OutsideValidity,
}

impl ProviderNativeInstrumentAttestation {
    /// Selects one exact provider identity and venue mapping from durable canonical evidence.
    pub fn try_select(
        input: ProviderNativeInstrumentAttestationInput<'_>,
    ) -> Result<Self, ProviderNativeInstrumentAttestationError> {
        let ProviderNativeInstrumentAttestationInput {
            definition,
            definition_revision_digest,
            definition_published_at,
            provider_key,
            venue_mapping,
            selected_at,
        } = input;
        require_sha256(definition_revision_digest)?;
        if definition_published_at > selected_at {
            return Err(ProviderNativeInstrumentAttestationError::FutureDefinition);
        }
        let reference_digest = definition.reference_payload_evidence().content_digest();
        require_sha256(reference_digest)?;
        let identity = definition
            .provider_identity_at(
                provider_key.source_id(),
                provider_key.provider_instrument_id(),
                selected_at,
            )
            .ok_or(ProviderNativeInstrumentAttestationError::ProviderIdentityUnavailable)?;
        if identity.instrument_id() != definition.instrument_id() {
            return Err(ProviderNativeInstrumentAttestationError::InstrumentMismatch);
        }
        if !definition.venue_mappings().contains(&venue_mapping) {
            return Err(ProviderNativeInstrumentAttestationError::VenueMappingMismatch);
        }
        let provider_identity_digest = identity.evidence().content_digest();
        require_sha256(provider_identity_digest)?;
        let identity_observed_at = identity
            .observation_timestamps()
            .iter()
            .rev()
            .copied()
            .find(|observed_at| *observed_at <= selected_at)
            .ok_or(ProviderNativeInstrumentAttestationError::FutureObservation)?;
        let definition_interval = definition.effective_interval();
        let identity_interval = identity.validity();
        let valid_from = definition_interval
            .starts_at()
            .max(identity_interval.starts_at());
        let valid_until = earliest_end(definition_interval.ends_at(), identity_interval.ends_at());
        if selected_at < valid_from || valid_until.is_some_and(|end| selected_at >= end) {
            return Err(ProviderNativeInstrumentAttestationError::OutsideValidity);
        }
        let attestation = Self {
            provider_key,
            instrument_id: definition.instrument_id(),
            venue_mapping,
            provider_identity_revision: identity.metadata_revision().clone(),
            provider_identity_digest,
            reference_revision: definition.reference_revision().clone(),
            reference_digest,
            definition_revision_digest,
            definition_published_at,
            identity_observed_at,
            valid_from,
            valid_until,
            selected_at,
        };
        attestation.validate_value()?;
        Ok(attestation)
    }

    /// Returns the exact source-qualified provider identity key.
    pub const fn provider_key(&self) -> &ProviderIdentityKey {
        &self.provider_key
    }

    /// Returns the stable canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact selected venue and venue-native symbol.
    pub const fn venue_mapping(&self) -> &VenueMapping {
        &self.venue_mapping
    }

    /// Returns the provider identity metadata revision.
    pub const fn provider_identity_revision(&self) -> &MetadataRevision {
        &self.provider_identity_revision
    }

    /// Returns the exact provider identity assertion digest.
    pub const fn provider_identity_digest(&self) -> EvidenceDigest {
        self.provider_identity_digest
    }

    /// Returns the canonical reference metadata revision.
    pub const fn reference_revision(&self) -> &MetadataRevision {
        &self.reference_revision
    }

    /// Returns the exact canonical reference payload digest.
    pub const fn reference_digest(&self) -> EvidenceDigest {
        self.reference_digest
    }

    /// Returns the exact durable definition revision digest.
    pub const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    /// Returns when the selected durable definition was published.
    pub const fn definition_published_at(&self) -> Timestamp {
        self.definition_published_at
    }

    /// Returns the latest retained identity observation no later than selection.
    pub const fn identity_observed_at(&self) -> Timestamp {
        self.identity_observed_at
    }

    /// Returns the inclusive intersection start of definition and identity validity.
    pub const fn valid_from(&self) -> Timestamp {
        self.valid_from
    }

    /// Returns the exclusive intersection end, when bounded.
    pub const fn valid_until(&self) -> Option<Timestamp> {
        self.valid_until
    }

    /// Returns the explicit point-in-time selection instant.
    pub const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }

    /// Validates that this attestation matches one normalized observation coordinate.
    pub fn validate_observation(
        &self,
        provider_key: &ProviderIdentityKey,
        venue_mapping: &VenueMapping,
        instrument_id: InstrumentId,
    ) -> Result<(), ProviderNativeInstrumentAttestationError> {
        self.validate_value()?;
        if self.provider_key != *provider_key || self.instrument_id != instrument_id {
            return Err(ProviderNativeInstrumentAttestationError::InstrumentMismatch);
        }
        if self.venue_mapping != *venue_mapping {
            return Err(ProviderNativeInstrumentAttestationError::VenueMappingMismatch);
        }
        Ok(())
    }

    /// Validates continued use at a later canonical observation instant.
    pub fn validate_at(
        &self,
        observed_at: Timestamp,
    ) -> Result<(), ProviderNativeInstrumentAttestationError> {
        self.validate_value()?;
        if observed_at < self.selected_at
            || observed_at < self.valid_from
            || self.valid_until.is_some_and(|end| observed_at >= end)
        {
            return Err(ProviderNativeInstrumentAttestationError::OutsideValidity);
        }
        Ok(())
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> usize {
        self.provider_key.source_id().as_str().len()
            + self.provider_key.provider_instrument_id().as_str().len()
            + self.venue_mapping.venue_id().as_str().len()
            + self.venue_mapping.venue_symbol().as_str().len()
            + self
                .provider_identity_revision
                .as_source_identifier()
                .as_str()
                .len()
            + self
                .reference_revision
                .as_source_identifier()
                .as_str()
                .len()
    }

    fn validate_value(&self) -> Result<(), ProviderNativeInstrumentAttestationError> {
        require_sha256(self.provider_identity_digest)?;
        require_sha256(self.reference_digest)?;
        require_sha256(self.definition_revision_digest)?;
        if self.definition_published_at > self.selected_at {
            return Err(ProviderNativeInstrumentAttestationError::FutureDefinition);
        }
        if self.identity_observed_at > self.selected_at {
            return Err(ProviderNativeInstrumentAttestationError::FutureObservation);
        }
        if self.selected_at < self.valid_from
            || self
                .valid_until
                .is_some_and(|end| end <= self.valid_from || self.selected_at >= end)
        {
            return Err(ProviderNativeInstrumentAttestationError::OutsideValidity);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderNativeInstrumentAttestationWire {
    provider_key: ProviderIdentityKey,
    instrument_id: InstrumentId,
    venue_mapping: VenueMapping,
    provider_identity_revision: MetadataRevision,
    provider_identity_digest: EvidenceDigest,
    reference_revision: MetadataRevision,
    reference_digest: EvidenceDigest,
    definition_revision_digest: EvidenceDigest,
    definition_published_at: Timestamp,
    identity_observed_at: Timestamp,
    valid_from: Timestamp,
    valid_until: Option<Timestamp>,
    selected_at: Timestamp,
}

impl<'de> Deserialize<'de> for ProviderNativeInstrumentAttestation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderNativeInstrumentAttestationWire::deserialize(deserializer)?;
        let value = Self {
            provider_key: wire.provider_key,
            instrument_id: wire.instrument_id,
            venue_mapping: wire.venue_mapping,
            provider_identity_revision: wire.provider_identity_revision,
            provider_identity_digest: wire.provider_identity_digest,
            reference_revision: wire.reference_revision,
            reference_digest: wire.reference_digest,
            definition_revision_digest: wire.definition_revision_digest,
            definition_published_at: wire.definition_published_at,
            identity_observed_at: wire.identity_observed_at,
            valid_from: wire.valid_from,
            valid_until: wire.valid_until,
            selected_at: wire.selected_at,
        };
        value.validate_value().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

fn require_sha256(digest: EvidenceDigest) -> Result<(), ProviderNativeInstrumentAttestationError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes().iter().all(|byte| *byte == 0)
    {
        Err(ProviderNativeInstrumentAttestationError::InvalidDigest)
    } else {
        Ok(())
    }
}

const fn earliest_end(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left.unix_nanos() <= right.unix_nanos() {
            left
        } else {
            right
        }),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use market_squawk_domain::{
        AssetClass, Currency, EffectiveInterval, ExactPayloadEvidence,
        MarketDataInstrumentDefinitionInput, ProviderIdentityEvidence, ProviderIdentityRecord,
        ProviderIdentityRecordInput, ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId,
        SourceIdentifier, VenueId, VenueSymbol,
    };

    use super::*;

    #[test]
    fn selection_accepts_exact_pit_identity_and_rejects_expired_or_mismatched_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let digest = |byte| EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]);
        let instrument_id = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
        let source_id = SourceId::try_from("current-market-source")?;
        let provider_instrument_id = ProviderInstrumentId::try_from("native-id")?;
        let venue_mapping = VenueMapping::new(
            VenueId::try_from("venue")?,
            VenueSymbol::try_from("SYMBOL")?,
        );
        let effective = EffectiveInterval::new(
            Timestamp::from_unix_nanos(10),
            Some(Timestamp::from_unix_nanos(30)),
        )?;
        let definition =
            MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                instrument_id,
                reference_evidence: RevisionBoundPayloadEvidence::new(
                    MetadataRevision::new(SourceIdentifier::try_from("reference-v1")?),
                    ExactPayloadEvidence::from_content_digest(digest(1)),
                ),
                effective_interval: effective,
                asset_class: AssetClass::Equity,
                display_name: None,
                quote_currency: Currency::try_from("USD")?,
                quote_currency_evidence: ExactPayloadEvidence::from_content_digest(digest(2)),
                venue_mappings: vec![venue_mapping.clone()],
                provider_identities: vec![ProviderIdentityRecord::new(
                    ProviderIdentityRecordInput {
                        instrument_id,
                        source_id: source_id.clone(),
                        provider_instrument_id: provider_instrument_id.clone(),
                        evidence: ProviderIdentityEvidence::from_content_digest(digest(3)),
                        source_timestamp: None,
                        observed_at: Timestamp::from_unix_nanos(12),
                        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                            "provider-identity-v1",
                        )?),
                        validity: effective,
                        supersedes: None,
                    },
                )],
                identifiers: Vec::new(),
            })?;
        let select = |selected_at, mapping| {
            ProviderNativeInstrumentAttestation::try_select(
                ProviderNativeInstrumentAttestationInput {
                    definition: &definition,
                    definition_revision_digest: digest(4),
                    definition_published_at: Timestamp::from_unix_nanos(11),
                    provider_key: ProviderIdentityKey::new(
                        source_id.clone(),
                        provider_instrument_id.clone(),
                    ),
                    venue_mapping: mapping,
                    selected_at: Timestamp::from_unix_nanos(selected_at),
                },
            )
        };
        let selected = select(20, venue_mapping.clone())?;
        assert_eq!(selected.instrument_id(), instrument_id);
        assert_eq!(selected.valid_until(), Some(Timestamp::from_unix_nanos(30)));
        assert_eq!(
            select(30, venue_mapping),
            Err(ProviderNativeInstrumentAttestationError::ProviderIdentityUnavailable)
        );
        assert_eq!(
            select(
                20,
                VenueMapping::new(
                    VenueId::try_from("other-venue")?,
                    VenueSymbol::try_from("SYMBOL")?,
                ),
            ),
            Err(ProviderNativeInstrumentAttestationError::VenueMappingMismatch)
        );
        Ok(())
    }
}
