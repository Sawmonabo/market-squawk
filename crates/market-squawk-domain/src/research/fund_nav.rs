//! Canonical daily fund/share-class NAV observations.

use std::fmt;
use std::num::NonZeroU64;

use serde::{Deserialize, Deserializer, Serialize};

use super::{ResearchError, require_instrument};
use crate::{
    CalendarDate, Currency, EvidenceDigest, ExactPayloadEvidence, MetadataRevision, Money,
    ProviderChannel, ProviderInstrumentId, ProviderProduct, ResearchContext, SourceIdentifier,
    Timestamp,
};

/// Source-neutral valuation basis for one fund NAV.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundNavValuationBasis {
    /// Exact net assets divided by the resolved share class's outstanding shares.
    PerShare,
}

/// Closed reason that an exact daily NAV has no observed amount.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundNavMissingState {
    /// The supported fund's NAV was not yet published when collected.
    NotYetPublished,
    /// The provider does not support the resolved fund/share class.
    Unsupported,
    /// The provider completed the response but omitted the supported NAV.
    SourceMissing,
    /// A returned source value could not satisfy the exact NAV contract.
    Invalid,
    /// Collection could not establish a complete source result.
    Unavailable,
}

/// Exact observed money or one closed missing state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum FundNavValue {
    /// Exact provider-reported NAV with currency.
    Observed(Money),
    /// No amount was admitted for the explicitly retained reason.
    Missing(FundNavMissingState),
}

/// Whether the exact request/page set was completely observed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundNavCompleteness {
    /// Every response component required by the contract was observed.
    Complete,
    /// The response set could not be proven complete.
    Incomplete,
}

/// Closed disposition of the provider request for this fund/date.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundNavDisposition {
    /// A valid NAV amount was returned.
    Returned,
    /// The supported NAV was not yet published.
    NotYetPublished,
    /// The resolved fund/share class is unsupported.
    Unsupported,
    /// A complete response omitted the supported NAV.
    SourceMissing,
    /// A returned value was invalid.
    Invalid,
    /// The request did not produce a complete authoritative result.
    Unavailable,
}

/// Exact public-or-gated entitlement evidence used for collection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FundNavEntitlementEvidence {
    /// Public access, still bound to exact policy/capability evidence.
    Public { evidence: EvidenceDigest },
    /// Gated access through one exact nonzero entitlement generation.
    Gated {
        generation: NonZeroU64,
        evidence: EvidenceDigest,
    },
}

impl FundNavEntitlementEvidence {
    /// Returns the generation only for gated access.
    pub const fn generation(self) -> Option<NonZeroU64> {
        match self {
            Self::Public { .. } => None,
            Self::Gated { generation, .. } => Some(generation),
        }
    }

    /// Returns exact evidence for the admitted entitlement decision.
    pub const fn evidence(self) -> EvidenceDigest {
        match self {
            Self::Public { evidence } | Self::Gated { evidence, .. } => evidence,
        }
    }
}

/// Exact native source-contract/schema identity used to decode one NAV row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundNavNativeSchema {
    source_contract_revision: MetadataRevision,
    source_contract_evidence: ExactPayloadEvidence,
    native_schema: SourceIdentifier,
    native_schema_revision: MetadataRevision,
    native_schema_evidence: ExactPayloadEvidence,
}

impl FundNavNativeSchema {
    /// Constructs exact contract and native-schema evidence.
    pub const fn new(
        source_contract_revision: MetadataRevision,
        source_contract_evidence: ExactPayloadEvidence,
        native_schema: SourceIdentifier,
        native_schema_revision: MetadataRevision,
        native_schema_evidence: ExactPayloadEvidence,
    ) -> Self {
        Self {
            source_contract_revision,
            source_contract_evidence,
            native_schema,
            native_schema_revision,
            native_schema_evidence,
        }
    }

    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    pub const fn source_contract_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_contract_evidence
    }

    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    pub const fn native_schema_revision(&self) -> &MetadataRevision {
        &self.native_schema_revision
    }

    pub const fn native_schema_evidence(&self) -> &ExactPayloadEvidence {
        &self.native_schema_evidence
    }
}

/// Request, page, raw-object/row, completeness, and entitlement lineage for one NAV.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FundNavLineage {
    native_schema: FundNavNativeSchema,
    entitlement: FundNavEntitlementEvidence,
    request_identity: EvidenceDigest,
    raw_object: ExactPayloadEvidence,
    raw_row: ExactPayloadEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_identity: Option<EvidenceDigest>,
    checkpoint_identity: EvidenceDigest,
    completeness: FundNavCompleteness,
    disposition: FundNavDisposition,
}

impl FundNavLineage {
    #[allow(
        clippy::too_many_arguments,
        reason = "every exact collection boundary stays explicit"
    )]
    pub fn try_new(
        native_schema: FundNavNativeSchema,
        entitlement: FundNavEntitlementEvidence,
        request_identity: EvidenceDigest,
        raw_object: ExactPayloadEvidence,
        raw_row: ExactPayloadEvidence,
        page_identity: Option<EvidenceDigest>,
        checkpoint_identity: EvidenceDigest,
        completeness: FundNavCompleteness,
        disposition: FundNavDisposition,
    ) -> Result<Self, ResearchError> {
        let digests = [
            entitlement.evidence(),
            request_identity,
            raw_object.content_digest(),
            raw_row.content_digest(),
            checkpoint_identity,
            native_schema.source_contract_evidence().content_digest(),
            native_schema.native_schema_evidence().content_digest(),
        ];
        if digests.into_iter().any(|digest| digest.bytes() == [0; 32])
            || page_identity.is_some_and(|digest| digest.bytes() == [0; 32])
        {
            return Err(ResearchError::InvalidFundNavLineage);
        }
        Ok(Self {
            native_schema,
            entitlement,
            request_identity,
            raw_object,
            raw_row,
            page_identity,
            checkpoint_identity,
            completeness,
            disposition,
        })
    }

    pub const fn native_schema(&self) -> &FundNavNativeSchema {
        &self.native_schema
    }

    pub const fn entitlement(&self) -> FundNavEntitlementEvidence {
        self.entitlement
    }

    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    pub const fn raw_object(&self) -> &ExactPayloadEvidence {
        &self.raw_object
    }

    pub const fn raw_row(&self) -> &ExactPayloadEvidence {
        &self.raw_row
    }

    pub const fn page_identity(&self) -> Option<EvidenceDigest> {
        self.page_identity
    }

    pub const fn checkpoint_identity(&self) -> EvidenceDigest {
        self.checkpoint_identity
    }

    pub const fn completeness(&self) -> FundNavCompleteness {
        self.completeness
    }

    pub const fn disposition(&self) -> FundNavDisposition {
        self.disposition
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundNavLineageWire {
    native_schema: FundNavNativeSchema,
    entitlement: FundNavEntitlementEvidence,
    request_identity: EvidenceDigest,
    raw_object: ExactPayloadEvidence,
    raw_row: ExactPayloadEvidence,
    page_identity: Option<EvidenceDigest>,
    checkpoint_identity: EvidenceDigest,
    completeness: FundNavCompleteness,
    disposition: FundNavDisposition,
}

impl<'de> Deserialize<'de> for FundNavLineage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundNavLineageWire::deserialize(deserializer)?;
        Self::try_new(
            wire.native_schema,
            wire.entitlement,
            wire.request_identity,
            wire.raw_object,
            wire.raw_row,
            wire.page_identity,
            wire.checkpoint_identity,
            wire.completeness,
            wire.disposition,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Source correction/finality and typed predecessor/successor evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundNavRevisionEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_revision: Option<SourceIdentifier>,
    correction: FundNavCorrectionState,
    finality: FundNavFinality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predecessor: Option<EvidenceDigest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    successor: Option<EvidenceDigest>,
}

impl FundNavRevisionEvidence {
    pub fn try_new(
        source_revision: Option<SourceIdentifier>,
        correction: FundNavCorrectionState,
        finality: FundNavFinality,
        predecessor: Option<EvidenceDigest>,
        successor: Option<EvidenceDigest>,
    ) -> Result<Self, ResearchError> {
        if predecessor.is_some_and(|digest| digest.bytes() == [0; 32])
            || successor.is_some_and(|digest| digest.bytes() == [0; 32])
            || predecessor.is_some() && predecessor == successor
        {
            return Err(ResearchError::InvalidFundNavRevisionEvidence);
        }
        Ok(Self {
            source_revision,
            correction,
            finality,
            predecessor,
            successor,
        })
    }

    pub const fn source_revision(&self) -> Option<&SourceIdentifier> {
        self.source_revision.as_ref()
    }

    pub const fn correction(&self) -> FundNavCorrectionState {
        self.correction
    }

    pub const fn finality(&self) -> FundNavFinality {
        self.finality
    }

    pub const fn predecessor(&self) -> Option<EvidenceDigest> {
        self.predecessor
    }

    pub const fn successor(&self) -> Option<EvidenceDigest> {
        self.successor
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundNavRevisionEvidenceWire {
    source_revision: Option<SourceIdentifier>,
    correction: FundNavCorrectionState,
    finality: FundNavFinality,
    predecessor: Option<EvidenceDigest>,
    successor: Option<EvidenceDigest>,
}

impl<'de> Deserialize<'de> for FundNavRevisionEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundNavRevisionEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.source_revision,
            wire.correction,
            wire.finality,
            wire.predecessor,
            wire.successor,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundNavCorrectionState {
    Original,
    Corrected,
    Unspecified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundNavFinality {
    Preliminary,
    Final,
    Unspecified,
}

/// Complete constructor input for one exact daily fund/share-class NAV.
#[derive(Debug)]
pub struct FundNavObservationInput {
    pub context: ResearchContext,
    pub provider_instrument_id: ProviderInstrumentId,
    pub instrument_reference_revision: MetadataRevision,
    pub provider_product: ProviderProduct,
    pub provider_channel: ProviderChannel,
    pub nav_date: CalendarDate,
    pub valuation_basis: FundNavValuationBasis,
    pub currency: Currency,
    pub value: FundNavValue,
    pub canonical_published_at: Timestamp,
    pub lineage: FundNavLineage,
    pub revision_evidence: FundNavRevisionEvidence,
}

/// Exact daily NAV for one already-resolved fund/share class.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FundNavObservation {
    context: ResearchContext,
    provider_instrument_id: ProviderInstrumentId,
    instrument_reference_revision: MetadataRevision,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    nav_date: CalendarDate,
    valuation_basis: FundNavValuationBasis,
    currency: Currency,
    value: FundNavValue,
    canonical_published_at: Timestamp,
    lineage: FundNavLineage,
    revision_evidence: FundNavRevisionEvidence,
}

impl FundNavObservation {
    /// Constructs one exact fund/share-class NAV without inventing a date timestamp or value.
    pub fn try_new(input: FundNavObservationInput) -> Result<Self, ResearchError> {
        let instrument = require_instrument(&input.context)?;
        let _exact_fund_share_class = instrument;
        if input.context.provenance().venue_id().is_some() {
            return Err(ResearchError::FundNavMustNotHaveVenue);
        }
        if input.context.time().effective().calendar_date_value() != Some(input.nav_date) {
            return Err(ResearchError::FundNavDateMismatch);
        }
        let source_publication_matches = match input.context.time().published() {
            Some(published) if published.exact_timestamp().is_some() => {
                published.exact_timestamp() == input.context.provenance().source_timestamp()
            }
            Some(published) if published.calendar_date_value().is_some() => {
                input.context.provenance().source_timestamp().is_none()
            }
            None => input.context.provenance().source_timestamp().is_none(),
            Some(_) => false,
        };
        if !source_publication_matches {
            return Err(ResearchError::FundNavSourcePublicationMismatch);
        }
        if input
            .context
            .provenance()
            .availability()
            .conservative_available_at()
            .is_none()
        {
            return Err(ResearchError::FundNavRequiresConservativeAvailability);
        }
        let provenance = input.context.provenance();
        if input.canonical_published_at < provenance.ingested_at()
            || input.canonical_published_at < provenance.received_at()
            || provenance
                .availability()
                .conservative_available_at()
                .is_some_and(|available| input.canonical_published_at < available)
        {
            return Err(ResearchError::FundNavCanonicalPublicationTooEarly);
        }
        validate_value_lineage(input.value, input.currency, &input.lineage)?;
        Ok(Self {
            context: input.context,
            provider_instrument_id: input.provider_instrument_id,
            instrument_reference_revision: input.instrument_reference_revision,
            provider_product: input.provider_product,
            provider_channel: input.provider_channel,
            nav_date: input.nav_date,
            valuation_basis: input.valuation_basis,
            currency: input.currency,
            value: input.value,
            canonical_published_at: input.canonical_published_at,
            lineage: input.lineage,
            revision_evidence: input.revision_evidence,
        })
    }

    pub const fn context(&self) -> &ResearchContext {
        &self.context
    }
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }
    pub const fn instrument_reference_revision(&self) -> &MetadataRevision {
        &self.instrument_reference_revision
    }
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }
    pub const fn nav_date(&self) -> CalendarDate {
        self.nav_date
    }
    pub const fn valuation_basis(&self) -> FundNavValuationBasis {
        self.valuation_basis
    }
    pub const fn currency(&self) -> Currency {
        self.currency
    }
    pub const fn value(&self) -> FundNavValue {
        self.value
    }
    pub const fn canonical_published_at(&self) -> Timestamp {
        self.canonical_published_at
    }
    pub const fn lineage(&self) -> &FundNavLineage {
        &self.lineage
    }
    pub const fn revision_evidence(&self) -> &FundNavRevisionEvidence {
        &self.revision_evidence
    }
}

fn validate_value_lineage(
    value: FundNavValue,
    currency: Currency,
    lineage: &FundNavLineage,
) -> Result<(), ResearchError> {
    let valid = match value {
        FundNavValue::Observed(money) => {
            money.currency() == currency
                && money.amount() > rust_decimal::Decimal::ZERO
                && lineage.completeness() == FundNavCompleteness::Complete
                && lineage.disposition() == FundNavDisposition::Returned
        }
        FundNavValue::Missing(missing) => {
            let disposition = match missing {
                FundNavMissingState::NotYetPublished => FundNavDisposition::NotYetPublished,
                FundNavMissingState::Unsupported => FundNavDisposition::Unsupported,
                FundNavMissingState::SourceMissing => FundNavDisposition::SourceMissing,
                FundNavMissingState::Invalid => FundNavDisposition::Invalid,
                FundNavMissingState::Unavailable => FundNavDisposition::Unavailable,
            };
            lineage.disposition() == disposition
                && (missing == FundNavMissingState::Unavailable
                    && lineage.completeness() == FundNavCompleteness::Incomplete
                    || missing != FundNavMissingState::Unavailable
                        && lineage.completeness() == FundNavCompleteness::Complete)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ResearchError::InvalidFundNavValueState)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundNavObservationWire {
    context: ResearchContext,
    provider_instrument_id: ProviderInstrumentId,
    instrument_reference_revision: MetadataRevision,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    nav_date: CalendarDate,
    valuation_basis: FundNavValuationBasis,
    currency: Currency,
    value: FundNavValue,
    canonical_published_at: Timestamp,
    lineage: FundNavLineage,
    revision_evidence: FundNavRevisionEvidence,
}

impl<'de> Deserialize<'de> for FundNavObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundNavObservationWire::deserialize(deserializer)?;
        Self::try_new(FundNavObservationInput {
            context: wire.context,
            provider_instrument_id: wire.provider_instrument_id,
            instrument_reference_revision: wire.instrument_reference_revision,
            provider_product: wire.provider_product,
            provider_channel: wire.provider_channel,
            nav_date: wire.nav_date,
            valuation_basis: wire.valuation_basis,
            currency: wire.currency,
            value: wire.value,
            canonical_published_at: wire.canonical_published_at,
            lineage: wire.lineage,
            revision_evidence: wire.revision_evidence,
        })
        .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for FundNavMissingState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotYetPublished => "not_yet_published",
            Self::Unsupported => "unsupported",
            Self::SourceMissing => "source_missing",
            Self::Invalid => "invalid",
            Self::Unavailable => "unavailable",
        })
    }
}
