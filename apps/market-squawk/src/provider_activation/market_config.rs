//! Evidence-bound construction of authenticated market-provider configurations.
//!
//! This boundary deliberately does not discover instruments, read the network, or mint stable
//! instrument identities. Display-only Alpaca routes consume stable FIGI-backed
//! market-data definitions plus source-qualified symbol evidence; they neither require nor retain
//! execution terms or accepted execution-provider identities. Authenticated Kraken level 3 keeps
//! the stricter execution-capable definition contract because decimal book updates require exact
//! tick and lot terms. No route derives an [`InstrumentId`](market_squawk_domain::InstrumentId)
//! from a ticker, listing row, or provider symbol.

use std::num::NonZeroUsize;

use market_squawk_adapter_alpaca::{
    ALPACA_BASIC_EQUITY_SYMBOL_LIMIT, ALPACA_BASIC_OPTION_SYMBOL_LIMIT, AlpacaError,
    AlpacaHistoricalEquityConfig, AlpacaIexBootSnapshotPolicy, AlpacaIexLiveConfig,
    AlpacaInstrumentMapping, AlpacaOptionMapping, AlpacaOptionsLiveConfig, AlpacaTransportLimits,
};
use market_squawk_adapter_kraken::{
    KrakenL3ClientTier, KrakenL3Config, KrakenL3ConfigError, KrakenL3Depth, KrakenL3MetadataError,
    KrakenL3MetadataInput, KrakenL3ProductMapping,
};
use market_squawk_data::{
    ListingReferenceRecord, ListingReferenceRightsState, MarketDataInstrumentRecord, RightsBasis,
    SourceOperation,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, AuthorizationBasis, CryptoProductType, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier,
    ExternalIdentifierRecord, Figi, IdentifierEntitlement, InstrumentDefinition,
    InstrumentExecutionTerms, InstrumentId, MarketDataInstrumentDefinition, MetadataRevision,
    ProviderIdentityRecord, ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId,
    SourceIdentifier, Timestamp, TradingStatus, VenueId,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BudgetPoolError, DataUseOperation, FreshnessPolicy,
    HttpRequestBounds, ProviderBudgetPolicy, ProviderRateDeclaration, SourceMetadata,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::application::ResearchRightsAuthority;
use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderMarketAccount,
};
use super::nasdaq_reference::NasdaqCurrentListing;
use super::openfigi_identity::{
    OpenFigiIdentityPublicationResult, OpenFigiIdentityPublicationStatus,
};

const MAX_PRIORITY_BINDINGS: usize = 256;
const KRAKEN_L3_PRODUCT_LIMIT: usize = 200;

const KRAKEN_VENUE: &str = "kraken";

const METADATA_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/authenticated-market-provider-metadata/v2\0";
const DISPLAY_METADATA_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/authenticated-display-market-provider-metadata/v2\0";
const HISTORICAL_METADATA_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/authenticated-historical-market-provider-metadata/v1\0";
const KRAKEN_CONFIGURED_SYMBOL_EVIDENCE_REVISION: &str =
    "market-squawk/kraken-configured-provisional-symbol/v1";

/// Exact producer required when one market-provider configuration authority is unavailable.
///
/// Display routes require stable FIGI identity plus source-qualified symbol evidence. Kraken L3
/// requires a canonical venue mapping and exact execution terms; accepted provider identity and
/// assigned-pair evidence remain mandatory for the strict binding, while the closed provisional
/// binding defers provider acceptance to runtime qualification. A reference row never mints the
/// stable instrument ID in either contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketConfigAuthorityRequirement {
    /// A stable FIGI-backed definition with no execution authority or terms.
    MarketDataInstrumentDefinitionReadCapability,
    /// Source-qualified listing or assigned-identifier evidence for a provisional symbol.
    SourceQualifiedSubscriptionSymbolEvidenceProducer,
    /// A current canonical definition, including exact revision-bound execution terms.
    InstrumentDefinitionReadCapability,
    /// An accepted provider-symbol assertion inside the canonical definition.
    CanonicalProviderIdentityProducer,
    /// A current catalog-minted Nasdaq listing-reference row for a listed equity or fund.
    NasdaqListingReferenceReadCapability,
    /// An assigned OCC identity retained by the canonical option definition.
    AssignedOccOptionReferenceProducer,
    /// An assigned ticker identity retained by the canonical index definition.
    AssignedIndexTickerReferenceProducer,
    /// An assigned venue-qualified spot-pair identity retained by the canonical crypto definition.
    AssignedCryptoPairReferenceProducer,
    /// A provider-logical venue mapping retained by the canonical definition.
    CanonicalProviderVenueMappingProducer,
}

impl std::fmt::Display for MarketConfigAuthorityRequirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MarketDataInstrumentDefinitionReadCapability => {
                "MarketDataInstrumentDefinitionReadCapability backed by the stable FIGI reference master"
            }
            Self::SourceQualifiedSubscriptionSymbolEvidenceProducer => {
                "a source-qualified listing or assigned-identifier producer for the bounded subscription symbol"
            }
            Self::InstrumentDefinitionReadCapability => {
                "InstrumentDefinitionReadCapability backed by the canonical security/reference master"
            }
            Self::CanonicalProviderIdentityProducer => {
                "the canonical security/reference-master producer of accepted ProviderIdentityRecord values"
            }
            Self::NasdaqListingReferenceReadCapability => {
                "ListingReferenceReadCapability backed by NasdaqReferenceCatalogService"
            }
            Self::AssignedOccOptionReferenceProducer => {
                "the canonical option-series producer of assigned OCC identifier evidence"
            }
            Self::AssignedIndexTickerReferenceProducer => {
                "the canonical benchmark producer of assigned index-ticker evidence"
            }
            Self::AssignedCryptoPairReferenceProducer => {
                "the canonical venue-product producer of assigned Kraken spot-pair evidence"
            }
            Self::CanonicalProviderVenueMappingProducer => {
                "the canonical security/reference-master producer of provider-logical venue mappings"
            }
        })
    }
}

/// Closed, deterministic ordering for a bounded live-subscription set.
///
/// This is subscription presentation/admission ordering only. It is never provider queue
/// priority, market sequence, execution priority, or a claim of greater data quality.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSubscriptionPriority {
    /// An instrument with an open position or active paper order.
    OpenPositionOrPaperOrder,
    /// A held instrument without a higher-priority active order/position reason.
    Holding,
    /// The instrument currently viewed by the user.
    CurrentlyViewed,
    /// An instrument in a user watchlist.
    Watchlist,
    /// An instrument required by an active bounded screen.
    ActiveScreen,
    /// One of the small, explicitly configured benchmark set.
    Benchmark,
}

/// Exact source-side coverage evidence for one logical provider/access surface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MarketSourceEvidence {
    source_id: SourceId,
    coverage_evidence: ExactPayloadEvidence,
    coverage_effective: EffectiveInterval,
    freshness: FreshnessPolicy,
}

impl MarketSourceEvidence {
    /// Retains explicit source identity, payload evidence, coverage time, and freshness policy.
    pub const fn new(
        source_id: SourceId,
        coverage_evidence: ExactPayloadEvidence,
        coverage_effective: EffectiveInterval,
        freshness: FreshnessPolicy,
    ) -> Self {
        Self {
            source_id,
            coverage_evidence,
            coverage_effective,
            freshness,
        }
    }

    /// Returns the distinct logical source identity registered for this access surface.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact evidence for the declared provider coverage.
    pub const fn coverage_evidence(&self) -> &ExactPayloadEvidence {
        &self.coverage_evidence
    }

    /// Returns the source-evidenced coverage interval.
    pub const fn coverage_effective(&self) -> EffectiveInterval {
        self.coverage_effective
    }

    /// Returns the explicit freshness limits for this logical source.
    pub const fn freshness(&self) -> FreshnessPolicy {
        self.freshness
    }
}

/// Security-level reference authority attached to one canonical definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketInstrumentReferenceBinding {
    /// A current, catalog-minted Nasdaq directory row used only as listing corroboration.
    NasdaqListing(ListingReferenceRecord),
    /// An assigned identifier record already retained by the canonical definition.
    AssignedExternalIdentifier(ExternalIdentifierRecord),
}

/// Source-qualified evidence for one provisional display-market subscription symbol.
///
/// The evidence proves where the symbol/listing assertion came from and binds it to an exact
/// payload. It does not claim that Alpaca has accepted the symbol. That separate claim
/// is established only by a provider-qualified subscription acknowledgement or first data frame
/// in the live runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MarketDataSubscriptionSymbolEvidence {
    kind: MarketDataSubscriptionSymbolEvidenceKind,
}

impl MarketDataSubscriptionSymbolEvidence {
    /// Returns the exact source namespace supporting the provisional symbol.
    pub const fn source_id(&self) -> &SourceId {
        match &self.kind {
            MarketDataSubscriptionSymbolEvidenceKind::NasdaqSessionListing {
                source_id, ..
            } => source_id,
            MarketDataSubscriptionSymbolEvidenceKind::AssignedExternalIdentifier { record } => {
                record.source_id()
            }
        }
    }

    /// Returns the exact source payload evidence supporting the provisional symbol.
    pub const fn source_payload_evidence(&self) -> &ExactPayloadEvidence {
        match &self.kind {
            MarketDataSubscriptionSymbolEvidenceKind::NasdaqSessionListing {
                source_payload_evidence,
                ..
            } => source_payload_evidence,
            MarketDataSubscriptionSymbolEvidenceKind::AssignedExternalIdentifier { record } => {
                record.source_evidence()
            }
        }
    }

    /// Returns the evidence interval that must contain the provider activation instant.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        match &self.kind {
            MarketDataSubscriptionSymbolEvidenceKind::NasdaqSessionListing {
                effective, ..
            } => *effective,
            MarketDataSubscriptionSymbolEvidenceKind::AssignedExternalIdentifier { record } => {
                record.validity()
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MarketDataSubscriptionSymbolEvidenceKind {
    NasdaqSessionListing {
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        source_payload_evidence: ExactPayloadEvidence,
        source_timestamp: Timestamp,
        observed_at: Timestamp,
        symbol: ProviderInstrumentId,
        mic: VenueId,
        asset_class: AssetClass,
        effective: EffectiveInterval,
    },
    AssignedExternalIdentifier {
        record: ExternalIdentifierRecord,
    },
}

/// Display-only binding between stable reference identity and one bounded subscription symbol.
///
/// Construction consumes a catalog-verified [`MarketDataInstrumentRecord`] and retains its exact
/// whole-definition digest in addition to the stable ID, FIGI, asset class, effective interval,
/// and revision-bound reference evidence. Tick size, lot size, multiplier, trading eligibility,
/// and accepted execution-provider identity are absent from this type by construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentBinding {
    priority: MarketSubscriptionPriority,
    instrument_id: InstrumentId,
    permanent_figi: Figi,
    asset_class: AssetClass,
    definition_reference_evidence: RevisionBoundPayloadEvidence,
    definition_effective: EffectiveInterval,
    definition_revision_digest: EvidenceDigest,
    subscription_symbol: ProviderInstrumentId,
    symbol_evidence: MarketDataSubscriptionSymbolEvidence,
}

impl MarketDataInstrumentBinding {
    /// Binds one session-only Nasdaq symbol to stable FIGI-backed reference identity.
    ///
    /// The definition and exact OpenFIGI publication result must describe the same current
    /// session listing. The constructor never requires a durable Nasdaq catalog row or venue
    /// mapping, never reads an accepted provider identity, and never derives the stable ID from
    /// `subscription_symbol`.
    ///
    /// # Errors
    ///
    /// Rejects a cross-bound listing/FIGI result, an incompatible equity/fund classification,
    /// invalid source timing, or any symbol other than the exact current-directory symbol.
    pub(crate) fn try_from_nasdaq_session_listing(
        priority: MarketSubscriptionPriority,
        definition_record: MarketDataInstrumentRecord,
        subscription_symbol: ProviderInstrumentId,
        listing: NasdaqCurrentListing,
        identity_result: &OpenFigiIdentityPublicationResult,
    ) -> Result<Self, MarketProviderConfigurationError> {
        let definition = definition_record.definition();
        validate_market_data_session_listing_binding(
            definition,
            subscription_symbol.as_str(),
            &listing,
            identity_result,
        )?;
        let symbol_evidence = MarketDataSubscriptionSymbolEvidence {
            kind: MarketDataSubscriptionSymbolEvidenceKind::NasdaqSessionListing {
                source_id: listing.source_id().clone(),
                metadata_revision: listing.metadata_revision().clone(),
                source_payload_evidence: listing.source_payload_evidence().clone(),
                source_timestamp: listing.source_timestamp(),
                observed_at: listing.observed_at(),
                symbol: listing.key().symbol().clone(),
                mic: listing.key().mic().clone(),
                asset_class: listing.asset_class(),
                effective: EffectiveInterval::new(listing.observed_at(), None)?,
            },
        };
        Ok(Self::from_validated_parts(
            priority,
            &definition_record,
            subscription_symbol,
            symbol_evidence,
        ))
    }

    /// Binds one assigned OCC/index symbol to stable FIGI-backed reference identity.
    ///
    /// # Errors
    ///
    /// Rejects evidence not retained by the definition, unverified or restricted assignment,
    /// incompatible asset/evidence families, or a symbol not exactly represented by the record.
    pub fn try_from_assigned_identifier(
        priority: MarketSubscriptionPriority,
        definition_record: MarketDataInstrumentRecord,
        subscription_symbol: ProviderInstrumentId,
        identifier_record: ExternalIdentifierRecord,
    ) -> Result<Self, MarketProviderConfigurationError> {
        let definition = definition_record.definition();
        if !definition
            .identifiers()
            .iter()
            .any(|accepted| accepted == &identifier_record)
            || identifier_record.assignment_verification()
                != AssignmentVerification::VerifiedAssigned
            || identifier_record.rights_policy().entitlement()
                == IdentifierEntitlement::UnknownOrRestricted
        {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement:
                    MarketConfigAuthorityRequirement::SourceQualifiedSubscriptionSymbolEvidenceProducer,
            });
        }
        validate_market_data_identifier_family(
            definition.instrument_id(),
            definition.asset_class(),
            &identifier_record,
        )?;
        validate_market_data_identifier_symbol(
            definition.instrument_id(),
            subscription_symbol.as_str(),
            &identifier_record,
        )?;
        let symbol_evidence = MarketDataSubscriptionSymbolEvidence {
            kind: MarketDataSubscriptionSymbolEvidenceKind::AssignedExternalIdentifier {
                record: identifier_record,
            },
        };
        Ok(Self::from_validated_parts(
            priority,
            &definition_record,
            subscription_symbol,
            symbol_evidence,
        ))
    }

    fn from_validated_parts(
        priority: MarketSubscriptionPriority,
        definition_record: &MarketDataInstrumentRecord,
        subscription_symbol: ProviderInstrumentId,
        symbol_evidence: MarketDataSubscriptionSymbolEvidence,
    ) -> Self {
        let definition = definition_record.definition();
        Self {
            priority,
            instrument_id: definition.instrument_id(),
            permanent_figi: definition.permanent_figi().clone(),
            asset_class: definition.asset_class(),
            definition_reference_evidence: definition.reference_evidence().clone(),
            definition_effective: definition.effective_interval(),
            definition_revision_digest: definition_record.revision_digest(),
            subscription_symbol,
            symbol_evidence,
        }
    }

    /// Returns the bounded subscription-priority reason.
    pub const fn priority(&self) -> MarketSubscriptionPriority {
        self.priority
    }

    /// Returns the stable internal ID supplied by the FIGI-backed definition authority.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the permanent FIGI that anchors stable reference identity.
    pub const fn permanent_figi(&self) -> &Figi {
        &self.permanent_figi
    }

    /// Returns the non-execution asset family supplied by the stable definition.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    /// Returns the exact reference-master revision and payload evidence.
    pub const fn definition_reference_evidence(&self) -> &RevisionBoundPayloadEvidence {
        &self.definition_reference_evidence
    }

    /// Returns the stable definition's half-open effective interval.
    pub const fn definition_effective(&self) -> EffectiveInterval {
        self.definition_effective
    }

    /// Returns the canonical SHA-256 digest of the complete immutable catalog definition.
    pub const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    /// Returns the bounded symbol proposed to the provider for this subscription.
    ///
    /// This symbol remains provisional until the live runtime qualifies the provider's
    /// acknowledgement or first data frame against the exact configured mapping.
    pub fn provisional_subscription_symbol(&self) -> &str {
        self.subscription_symbol.as_str()
    }

    /// Returns the exact source-qualified evidence supporting the provisional symbol.
    pub const fn symbol_evidence(&self) -> &MarketDataSubscriptionSymbolEvidence {
        &self.symbol_evidence
    }
}

/// Non-empty, deterministically ordered, bounded display-market subscription inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedMarketDataInstrumentSet {
    bindings: Vec<MarketDataInstrumentBinding>,
}

impl BoundedMarketDataInstrumentSet {
    /// Validates the process-wide ceiling before provider-specific limits are applied.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized sets and duplicate stable IDs or subscription symbols.
    pub fn try_new(
        mut bindings: Vec<MarketDataInstrumentBinding>,
    ) -> Result<Self, MarketProviderConfigurationError> {
        if bindings.is_empty() || bindings.len() > MAX_PRIORITY_BINDINGS {
            return Err(MarketProviderConfigurationError::InstrumentSetBound {
                surface: "display-market-provider-priority-set",
                maximum: MAX_PRIORITY_BINDINGS,
            });
        }
        bindings.sort_by(compare_market_data_bindings);
        for (index, binding) in bindings.iter().enumerate() {
            if bindings[..index].iter().any(|prior| {
                prior.instrument_id() == binding.instrument_id()
                    || prior.provisional_subscription_symbol()
                        == binding.provisional_subscription_symbol()
            }) {
                return Err(MarketProviderConfigurationError::DuplicateInstrumentBinding);
            }
        }
        Ok(Self { bindings })
    }

    /// Returns the deterministic display-market subscription order.
    pub fn bindings(&self) -> &[MarketDataInstrumentBinding] {
        &self.bindings
    }

    fn into_bindings(self) -> Vec<MarketDataInstrumentBinding> {
        self.bindings
    }
}

/// One explicit provider-symbol/canonical-instrument binding.
///
/// The stable instrument ID and exact execution terms always come from `definition`. A strict
/// binding retains accepted provider-identity and reference evidence. The closed Kraken
/// provisional variant retains only an exact symbol already present in the definition's Kraken
/// venue mapping; runtime acknowledgement and a checksum-valid snapshot remain mandatory before
/// provider qualification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketInstrumentBinding {
    priority: MarketSubscriptionPriority,
    definition: InstrumentDefinition,
    symbol_authority: MarketInstrumentSymbolAuthority,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MarketInstrumentSymbolAuthority {
    AcceptedProviderIdentity {
        provider_identity: ProviderIdentityRecord,
        reference: MarketInstrumentReferenceBinding,
    },
    KrakenConfiguredProvisional {
        provider_symbol: ProviderInstrumentId,
    },
}

impl MarketInstrumentBinding {
    /// Binds independently obtained canonical and reference authorities without inferring an ID.
    ///
    /// Callers must obtain `definition` through [`market_squawk_data::InstrumentDefinitionReadCapability`].
    /// Listed-security rows must come from [`market_squawk_data::ListingReferenceReadCapability`].
    /// Option, index, and crypto identifiers must already be assigned records inside the canonical
    /// definition. The central constructor performs the time-, surface-, and provider-specific
    /// checks against the exact active lease.
    ///
    /// # Errors
    ///
    /// Rejects a provider record not accepted by the supplied definition, a reference record not
    /// retained by that definition, an incompatible asset/reference family, or a listing row that
    /// cannot be corroborated by a current canonical listing-venue mapping.
    pub fn try_new(
        priority: MarketSubscriptionPriority,
        definition: InstrumentDefinition,
        provider_identity: ProviderIdentityRecord,
        reference: MarketInstrumentReferenceBinding,
    ) -> Result<Self, MarketProviderConfigurationError> {
        if provider_identity.instrument_id() != definition.instrument_id()
            || !definition
                .provider_identities()
                .iter()
                .any(|accepted| accepted == &provider_identity)
        {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement: MarketConfigAuthorityRequirement::CanonicalProviderIdentityProducer,
            });
        }
        validate_reference_binding(&definition, &reference)?;
        Ok(Self {
            priority,
            definition,
            symbol_authority: MarketInstrumentSymbolAuthority::AcceptedProviderIdentity {
                provider_identity,
                reference,
            },
        })
    }

    /// Binds exact Kraken execution terms to a configured symbol without asserting provider
    /// acceptance or external identifier assignment.
    ///
    /// The definition remains the sole source of stable identity and tick/lot terms. This
    /// constructor accepts only a crypto definition whose existing `kraken` venue mapping exactly
    /// equals `provider_symbol`. The result remains provisional until the authenticated runtime
    /// admits the exact subscription acknowledgement and checksum-valid snapshot.
    ///
    /// # Errors
    ///
    /// Rejects a non-crypto definition or any missing/mismatched Kraken venue mapping.
    pub fn try_new_provisional_kraken(
        priority: MarketSubscriptionPriority,
        definition: InstrumentDefinition,
        provider_symbol: ProviderInstrumentId,
    ) -> Result<Self, MarketProviderConfigurationError> {
        if definition.asset_class() != AssetClass::Crypto {
            return Err(MarketProviderConfigurationError::UnsupportedAssetClass {
                instrument: definition.instrument_id(),
                asset_class: definition.asset_class(),
            });
        }
        let kraken = VenueId::try_from(KRAKEN_VENUE)?;
        if !definition.venue_mappings().iter().any(|mapping| {
            mapping.venue_id() == &kraken
                && mapping.venue_symbol().as_str() == provider_symbol.as_str()
        }) {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement:
                    MarketConfigAuthorityRequirement::CanonicalProviderVenueMappingProducer,
            });
        }
        Ok(Self {
            priority,
            definition,
            symbol_authority: MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional {
                provider_symbol,
            },
        })
    }

    /// Returns the bounded subscription-priority reason.
    pub const fn priority(&self) -> MarketSubscriptionPriority {
        self.priority
    }

    /// Returns the complete canonical definition and exact execution terms.
    pub const fn definition(&self) -> &InstrumentDefinition {
        &self.definition
    }

    /// Returns accepted provider identity when this is the strict evidence-bearing variant.
    pub const fn provider_identity(&self) -> Option<&ProviderIdentityRecord> {
        match &self.symbol_authority {
            MarketInstrumentSymbolAuthority::AcceptedProviderIdentity {
                provider_identity, ..
            } => Some(provider_identity),
            MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional { .. } => None,
        }
    }

    /// Returns strict listing/identifier corroboration when supplied by accepted authority.
    pub const fn reference(&self) -> Option<&MarketInstrumentReferenceBinding> {
        match &self.symbol_authority {
            MarketInstrumentSymbolAuthority::AcceptedProviderIdentity { reference, .. } => {
                Some(reference)
            }
            MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional { .. } => None,
        }
    }

    /// Returns the stable instrument ID supplied by the canonical definition.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.definition.instrument_id()
    }

    /// Returns the exact revision-bound terms required by order-level normalization.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.definition.execution_terms()
    }

    /// Returns the exact accepted or configured-provisional provider symbol.
    pub fn provider_symbol(&self) -> &str {
        match &self.symbol_authority {
            MarketInstrumentSymbolAuthority::AcceptedProviderIdentity {
                provider_identity, ..
            } => provider_identity.provider_instrument_id().as_str(),
            MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional { provider_symbol } => {
                provider_symbol.as_str()
            }
        }
    }

    /// Returns whether runtime provider qualification is still required for this symbol.
    pub const fn provider_symbol_is_provisional(&self) -> bool {
        matches!(
            &self.symbol_authority,
            MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional { .. }
        )
    }
}

/// Non-empty, deterministically ordered, bounded subscription inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedMarketInstrumentSet {
    bindings: Vec<MarketInstrumentBinding>,
}

impl BoundedMarketInstrumentSet {
    /// Validates the process-wide maximum before provider-specific ceilings are applied.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized sets and duplicate stable IDs or provider symbols. The retained
    /// order is priority, stable instrument ID, then exact provider symbol.
    pub fn try_new(
        mut bindings: Vec<MarketInstrumentBinding>,
    ) -> Result<Self, MarketProviderConfigurationError> {
        if bindings.is_empty() || bindings.len() > MAX_PRIORITY_BINDINGS {
            return Err(MarketProviderConfigurationError::InstrumentSetBound {
                surface: "market-provider-priority-set",
                maximum: MAX_PRIORITY_BINDINGS,
            });
        }
        bindings.sort_by(compare_bindings);
        for (index, binding) in bindings.iter().enumerate() {
            if bindings[..index].iter().any(|prior| {
                prior.instrument_id() == binding.instrument_id()
                    || prior.provider_symbol() == binding.provider_symbol()
            }) {
                return Err(MarketProviderConfigurationError::DuplicateInstrumentBinding);
            }
        }
        Ok(Self { bindings })
    }

    /// Returns the canonical bounded order used by subscriptions and Kraken rate batches.
    pub fn bindings(&self) -> &[MarketInstrumentBinding] {
        &self.bindings
    }

    fn into_bindings(self) -> Vec<MarketInstrumentBinding> {
        self.bindings
    }
}

/// Explicit Alpaca Basic construction inputs.
#[derive(Clone, Debug)]
pub struct AlpacaBasicMarketConfigurationInput {
    /// Trusted caller instant at which every retained authority is jointly revalidated.
    pub configured_at: Timestamp,
    pub iex_evidence: MarketSourceEvidence,
    pub options_evidence: Option<MarketSourceEvidence>,
    pub iex_instruments: BoundedMarketDataInstrumentSet,
    pub option_instruments: Option<BoundedMarketDataInstrumentSet>,
    pub transport_limits: AlpacaTransportLimits,
}

/// Explicit authenticated Kraken level-3 construction inputs.
#[derive(Clone, Debug)]
pub struct KrakenL3MarketConfigurationInput {
    /// Trusted caller instant at which every retained authority is jointly revalidated.
    pub configured_at: Timestamp,
    pub evidence: MarketSourceEvidence,
    pub instruments: BoundedMarketInstrumentSet,
    pub retained_depth: KrakenL3Depth,
    pub client_tier: KrakenL3ClientTier,
    pub max_message_bytes: NonZeroUsize,
}

/// Closed account-market request paired with an exact active activation lease.
#[derive(Clone, Debug)]
pub enum ProviderMarketConfigurationRequest {
    AlpacaBasic(AlpacaBasicMarketConfigurationInput),
    KrakenLevel3(KrakenL3MarketConfigurationInput),
}

/// Prepared Alpaca configs plus the exact lease and canonical definitions that produced them.
#[derive(Clone, Debug)]
pub struct PreparedAlpacaBasicMarketConfiguration {
    lease: ProviderActivationLease,
    account: ProviderAccountBinding,
    iex: AlpacaIexLiveConfig,
    iex_instruments: Box<[MarketDataInstrumentBinding]>,
    historical_metadata: SourceMetadata,
    historical_request_bounds: HttpRequestBounds,
    historical_rights: ResearchRightsAuthority,
    options: Option<(AlpacaOptionsLiveConfig, Box<[MarketDataInstrumentBinding]>)>,
}

impl PreparedAlpacaBasicMarketConfiguration {
    pub const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    pub const fn account_binding(&self) -> &ProviderAccountBinding {
        &self.account
    }

    pub const fn iex_config(&self) -> &AlpacaIexLiveConfig {
        &self.iex
    }

    pub fn options_config(&self) -> Option<&AlpacaOptionsLiveConfig> {
        self.options.as_ref().map(|(config, _bindings)| config)
    }

    pub fn iex_instruments(&self) -> &[MarketDataInstrumentBinding] {
        &self.iex_instruments
    }

    pub fn option_instruments(&self) -> Option<&[MarketDataInstrumentBinding]> {
        self.options
            .as_ref()
            .map(|(_config, bindings)| bindings.as_ref())
    }

    pub const fn historical_metadata(&self) -> &SourceMetadata {
        &self.historical_metadata
    }

    pub const fn historical_request_bounds(&self) -> HttpRequestBounds {
        self.historical_request_bounds
    }

    pub const fn historical_rights(&self) -> &ResearchRightsAuthority {
        &self.historical_rights
    }

    /// Moves the exact lease, configs, and route-definition inputs into central composition.
    #[allow(
        clippy::type_complexity,
        reason = "the exact handoff remains one atomic value tuple"
    )]
    pub fn into_parts(
        self,
    ) -> (
        ProviderActivationLease,
        AlpacaIexLiveConfig,
        Box<[MarketDataInstrumentBinding]>,
        SourceMetadata,
        HttpRequestBounds,
        ResearchRightsAuthority,
        Option<(AlpacaOptionsLiveConfig, Box<[MarketDataInstrumentBinding]>)>,
    ) {
        (
            self.lease,
            self.iex,
            self.iex_instruments,
            self.historical_metadata,
            self.historical_request_bounds,
            self.historical_rights,
            self.options,
        )
    }
}

/// Prepared authenticated Kraken L3 config with exact per-symbol tick/lot terms.
#[derive(Clone, Debug)]
pub struct PreparedKrakenL3MarketConfiguration {
    lease: ProviderActivationLease,
    account: ProviderAccountBinding,
    config: KrakenL3Config,
    instruments: Box<[MarketInstrumentBinding]>,
}

impl PreparedKrakenL3MarketConfiguration {
    pub const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    pub const fn account_binding(&self) -> &ProviderAccountBinding {
        &self.account
    }

    pub const fn config(&self) -> &KrakenL3Config {
        &self.config
    }

    pub fn instruments(&self) -> &[MarketInstrumentBinding] {
        &self.instruments
    }

    /// Resolves the exact canonical terms required by Kraken L3 decimal-to-tick/lot scaling.
    pub fn execution_terms_for(&self, provider_symbol: &str) -> Option<InstrumentExecutionTerms> {
        self.instruments
            .iter()
            .find(|binding| binding.provider_symbol() == provider_symbol)
            .map(MarketInstrumentBinding::execution_terms)
    }

    /// Moves the exact lease, config, and order-level route-definition inputs into composition.
    pub fn into_parts(
        self,
    ) -> (
        ProviderActivationLease,
        KrakenL3Config,
        Box<[MarketInstrumentBinding]>,
    ) {
        (self.lease, self.config, self.instruments)
    }
}

/// One exact account-provider configuration prepared without secrets or network access.
#[derive(Clone, Debug)]
pub enum PreparedMarketProviderConfiguration {
    AlpacaBasic(PreparedAlpacaBasicMarketConfiguration),
    KrakenLevel3(PreparedKrakenL3MarketConfiguration),
}

impl ProviderAdapterActivation {
    /// Constructs adapter configurations while holding exact active-lease mutation authority.
    ///
    /// This method performs no network access, reads no credential, and retains no secret. It
    /// validates the exact active lease before and after construction while the onboarding
    /// mutation guard is held. Every metadata revision is a deterministic SHA-256 commitment to
    /// the lease, qualified account budget, logical source evidence, provider profile, transport
    /// bounds, caller-supplied configuration instant, and sorted canonical instrument bindings.
    ///
    /// # Errors
    ///
    /// Fails closed for a stale/mismatched lease, an out-of-authority configuration instant,
    /// missing canonical security-level authority, stale strict provider identity, incompatible
    /// listing/identifier evidence, duplicated/unbounded priority set, logical-source conflation,
    /// or adapter contract failure.
    pub(crate) fn try_construct_market_provider_configuration(
        &self,
        lease: ProviderActivationLease,
        request: ProviderMarketConfigurationRequest,
    ) -> Result<PreparedMarketProviderConfiguration, MarketProviderConfigurationError> {
        let authority = self.onboarding.try_acquire_runtime_mutation_authority()?;
        authority.require_active(&lease)?;
        let configured = match request {
            ProviderMarketConfigurationRequest::AlpacaBasic(input) => {
                PreparedMarketProviderConfiguration::AlpacaBasic(prepare_alpaca(&lease, input)?)
            }
            ProviderMarketConfigurationRequest::KrakenLevel3(input) => {
                PreparedMarketProviderConfiguration::KrakenLevel3(prepare_kraken_l3(&lease, input)?)
            }
        };
        authority.require_active(&lease)?;
        Ok(configured)
    }

    /// Constructs a configuration for one private, unpublished prepared-runtime stage.
    pub(crate) fn try_construct_staged_market_provider_configuration(
        &self,
        lease: ProviderActivationLease,
        request: ProviderMarketConfigurationRequest,
    ) -> Result<PreparedMarketProviderConfiguration, MarketProviderConfigurationError> {
        let authority = self.onboarding.try_acquire_runtime_mutation_authority()?;
        authority.require_prepared_or_active(&lease)?;
        let configured = match request {
            ProviderMarketConfigurationRequest::AlpacaBasic(input) => {
                PreparedMarketProviderConfiguration::AlpacaBasic(prepare_alpaca(&lease, input)?)
            }
            ProviderMarketConfigurationRequest::KrakenLevel3(input) => {
                PreparedMarketProviderConfiguration::KrakenLevel3(prepare_kraken_l3(&lease, input)?)
            }
        };
        authority.require_prepared_or_active(&lease)?;
        Ok(configured)
    }
}

fn prepare_alpaca(
    lease: &ProviderActivationLease,
    input: AlpacaBasicMarketConfigurationInput,
) -> Result<PreparedAlpacaBasicMarketConfiguration, MarketProviderConfigurationError> {
    validate_configured_at(lease, input.configured_at)?;
    let account =
        ProviderAccountBinding::try_from_lease(ProviderMarketAccount::AlpacaBasic, lease)?;
    validate_source_evidence(input.configured_at, &input.iex_evidence)?;
    validate_set_bound(
        "alpaca-basic-iex",
        input.iex_instruments.bindings(),
        ALPACA_BASIC_EQUITY_SYMBOL_LIMIT,
    )?;
    validate_market_data_bindings(
        input.configured_at,
        input.iex_instruments.bindings(),
        |class| matches!(class, AssetClass::Equity | AssetClass::Fund),
    )?;
    let options_configured = match (&input.options_evidence, &input.option_instruments) {
        (Some(evidence), Some(instruments)) => {
            require_distinct_source_ids(&[&input.iex_evidence, evidence])?;
            validate_source_evidence(input.configured_at, evidence)?;
            validate_set_bound(
                "alpaca-basic-indicative-options",
                instruments.bindings(),
                ALPACA_BASIC_OPTION_SYMBOL_LIMIT,
            )?;
            validate_market_data_bindings(input.configured_at, instruments.bindings(), |class| {
                class == AssetClass::Option
            })?;
            true
        }
        (None, None) => false,
        _ => {
            return Err(
                MarketProviderConfigurationError::OptionalCapabilityBinding {
                    surface: "alpaca-basic-indicative-options",
                },
            );
        }
    };
    let budget = qualified_budget(lease, &account)?;
    let authorization = authorization(lease, &account)?;
    let boot_snapshot = AlpacaIexBootSnapshotPolicy::from_transport_limits(input.transport_limits);
    let iex_digest = display_metadata_digest(
        lease,
        input.configured_at,
        &account,
        &budget,
        &input.iex_evidence,
        MetadataProfile::AlpacaIex {
            indicative_options_configured: options_configured,
            boot_snapshot_revision: boot_snapshot.revision(),
            boot_snapshot_maximum_body_bytes: boot_snapshot.maximum_body_bytes(),
            boot_snapshot_total_timeout_nanos: duration_nanos_saturating(
                boot_snapshot.total_timeout(),
            ),
        },
        input.iex_instruments.bindings(),
        &AlpacaLimitsWire::from(input.transport_limits),
    )?;
    let iex_mappings = input
        .iex_instruments
        .bindings()
        .iter()
        .map(|binding| {
            AlpacaInstrumentMapping::try_new(
                binding.provisional_subscription_symbol().to_owned(),
                binding.instrument_id(),
                binding.asset_class(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let iex = AlpacaIexLiveConfig::try_new(
        input.iex_evidence.source_id.clone(),
        revision_evidence(lease, "alpaca-iex", iex_digest)?,
        authorization.clone(),
        input.iex_evidence.coverage_evidence.clone(),
        input.iex_evidence.coverage_effective,
        iex_mappings.clone(),
        input.iex_evidence.freshness,
        budget.clone(),
        input.transport_limits,
    )?;
    let historical_request_bounds = HttpRequestBounds::default();
    let historical_digest = metadata_digest_for_bindings(
        HISTORICAL_METADATA_EVIDENCE_DOMAIN,
        lease,
        input.configured_at,
        &account,
        &budget,
        &input.iex_evidence,
        MetadataProfile::AlpacaHistoricalIexDaily,
        input
            .iex_instruments
            .bindings()
            .iter()
            .map(MarketDataBindingEvidenceWire::from)
            .collect::<Vec<_>>(),
        &historical_request_bounds,
    )?;
    let historical_metadata = AlpacaHistoricalEquityConfig::try_parent_metadata(
        input.iex_evidence.source_id.clone(),
        revision_evidence(lease, "alpaca-iex-history", historical_digest)?,
        authorization.clone(),
        input.iex_evidence.coverage_evidence.clone(),
        input.iex_evidence.coverage_effective,
        iex_mappings.clone(),
        input.iex_evidence.freshness,
        budget.clone(),
        historical_request_bounds,
    )?;
    let historical_rights =
        alpaca_historical_research_rights(lease, historical_metadata.source_id())?;
    let options = match (input.options_evidence, input.option_instruments) {
        (Some(evidence), Some(instruments)) => {
            let digest = display_metadata_digest(
                lease,
                input.configured_at,
                &account,
                &budget,
                &evidence,
                MetadataProfile::AlpacaIndicativeOptions,
                instruments.bindings(),
                &AlpacaLimitsWire::from(input.transport_limits),
            )?;
            let mappings = instruments
                .bindings()
                .iter()
                .map(|binding| {
                    AlpacaOptionMapping::try_new(
                        binding.provisional_subscription_symbol().to_owned(),
                        binding.instrument_id(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let config = AlpacaOptionsLiveConfig::try_new(
                evidence.source_id.clone(),
                revision_evidence(lease, "alpaca-options", digest)?,
                authorization,
                evidence.coverage_evidence.clone(),
                evidence.coverage_effective,
                mappings,
                evidence.freshness,
                budget,
                input.transport_limits,
            )?;
            Some((config, instruments.into_bindings().into_boxed_slice()))
        }
        (None, None) => None,
        _ => {
            return Err(
                MarketProviderConfigurationError::OptionalCapabilityBinding {
                    surface: "alpaca-basic-indicative-options",
                },
            );
        }
    };
    Ok(PreparedAlpacaBasicMarketConfiguration {
        lease: lease.clone(),
        account,
        iex,
        iex_instruments: input.iex_instruments.into_bindings().into_boxed_slice(),
        historical_metadata,
        historical_request_bounds,
        historical_rights,
        options,
    })
}

fn prepare_kraken_l3(
    lease: &ProviderActivationLease,
    input: KrakenL3MarketConfigurationInput,
) -> Result<PreparedKrakenL3MarketConfiguration, MarketProviderConfigurationError> {
    validate_configured_at(lease, input.configured_at)?;
    let account =
        ProviderAccountBinding::try_from_lease(ProviderMarketAccount::KrakenLevel3, lease)?;
    validate_source_evidence(input.configured_at, &input.evidence)?;
    validate_set_bound(
        "kraken-authenticated-level3",
        input.instruments.bindings(),
        KRAKEN_L3_PRODUCT_LIMIT,
    )?;
    validate_bindings(
        input.configured_at,
        input.instruments.bindings(),
        KRAKEN_VENUE,
        |class| class == AssetClass::Crypto,
    )?;
    let budget = qualified_budget(lease, &account)?;
    let authorization = authorization(lease, &account)?;
    let digest = metadata_digest(
        lease,
        input.configured_at,
        &account,
        &budget,
        &input.evidence,
        MetadataProfile::kraken(input.retained_depth, input.client_tier),
        input.instruments.bindings(),
        &KrakenLimitsWire {
            max_message_bytes: input.max_message_bytes.get(),
        },
    )?;
    let products = input
        .instruments
        .bindings()
        .iter()
        .map(|binding| {
            KrakenL3ProductMapping::try_new(
                binding.provider_symbol().to_owned(),
                binding.instrument_id(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let instrument_ids = input
        .instruments
        .bindings()
        .iter()
        .map(MarketInstrumentBinding::instrument_id)
        .collect();
    let metadata = KrakenL3MetadataInput::new(
        input.evidence.source_id.clone(),
        revision_evidence(lease, "kraken-l3", digest)?,
        authorization,
        input.evidence.coverage_evidence.clone(),
        input.evidence.coverage_effective,
        instrument_ids,
        input.evidence.freshness,
        budget,
    )
    .try_build()?;
    let config = KrakenL3Config::try_new(
        metadata,
        products,
        input.retained_depth,
        input.client_tier,
        account.subject().clone(),
        input.max_message_bytes,
    )?;
    Ok(PreparedKrakenL3MarketConfiguration {
        lease: lease.clone(),
        account,
        config,
        instruments: input.instruments.into_bindings().into_boxed_slice(),
    })
}

fn validate_market_data_session_listing_binding(
    definition: &MarketDataInstrumentDefinition,
    subscription_symbol: &str,
    listing: &NasdaqCurrentListing,
    identity_result: &OpenFigiIdentityPublicationResult,
) -> Result<(), MarketProviderConfigurationError> {
    let OpenFigiIdentityPublicationStatus::Exact {
        candidate,
        instrument_id,
        catalog_disposition: _,
    } = identity_result.status()
    else {
        return Err(MarketProviderConfigurationError::InvalidListingBinding {
            instrument: definition.instrument_id(),
        });
    };
    if !matches!(listing.asset_class(), AssetClass::Equity | AssetClass::Fund)
        || listing.asset_class() != definition.asset_class()
        || identity_result.listing() != listing.key()
        || *instrument_id != definition.instrument_id()
        || candidate.exchange_figi() != definition.permanent_figi()
        || listing.key().symbol().as_str() != subscription_symbol
        || listing.source_timestamp() > listing.observed_at()
    {
        return Err(MarketProviderConfigurationError::InvalidListingBinding {
            instrument: definition.instrument_id(),
        });
    }
    Ok(())
}

fn validate_market_data_identifier_family(
    instrument: InstrumentId,
    asset_class: AssetClass,
    record: &ExternalIdentifierRecord,
) -> Result<(), MarketProviderConfigurationError> {
    if matches!(
        (asset_class, record.identifier()),
        (AssetClass::Option, ExternalIdentifier::OccOption(_))
            | (AssetClass::Index, ExternalIdentifier::Ticker(_))
    ) {
        Ok(())
    } else {
        Err(MarketProviderConfigurationError::InvalidIdentifierBinding { instrument })
    }
}

fn validate_market_data_identifier_symbol(
    instrument: InstrumentId,
    subscription_symbol: &str,
    record: &ExternalIdentifierRecord,
) -> Result<(), MarketProviderConfigurationError> {
    let exact_symbol = match record.identifier() {
        ExternalIdentifier::OccOption(identity) => {
            let suffix = identity
                .as_str()
                .get(6..)
                .ok_or(MarketProviderConfigurationError::InvalidIdentifierBinding { instrument })?;
            let mut compact = String::with_capacity(identity.root().len() + suffix.len());
            compact.push_str(identity.root());
            compact.push_str(suffix);
            compact == subscription_symbol
        }
        ExternalIdentifier::Ticker(ticker) => ticker.as_str() == subscription_symbol,
        _ => false,
    };
    if exact_symbol {
        Ok(())
    } else {
        Err(MarketProviderConfigurationError::InvalidIdentifierBinding { instrument })
    }
}

fn validate_reference_binding(
    definition: &InstrumentDefinition,
    reference: &MarketInstrumentReferenceBinding,
) -> Result<(), MarketProviderConfigurationError> {
    match (definition.asset_class(), reference) {
        (
            AssetClass::Equity | AssetClass::Fund,
            MarketInstrumentReferenceBinding::NasdaqListing(listing),
        ) => validate_listing_binding(definition, listing),
        (
            AssetClass::Option | AssetClass::Index | AssetClass::Crypto,
            MarketInstrumentReferenceBinding::AssignedExternalIdentifier(identifier),
        ) => {
            if !definition
                .identifiers()
                .iter()
                .any(|record| record == identifier)
            {
                return Err(MarketProviderConfigurationError::AuthorityRequired {
                    instrument: Some(definition.instrument_id()),
                    requirement: identifier_requirement(definition.asset_class()),
                });
            }
            validate_identifier_family(definition, identifier)
        }
        (AssetClass::Equity | AssetClass::Fund, _) => {
            Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement: MarketConfigAuthorityRequirement::NasdaqListingReferenceReadCapability,
            })
        }
        (AssetClass::Option | AssetClass::Index | AssetClass::Crypto, _) => {
            Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement: identifier_requirement(definition.asset_class()),
            })
        }
        _ => Err(MarketProviderConfigurationError::UnsupportedAssetClass {
            instrument: definition.instrument_id(),
            asset_class: definition.asset_class(),
        }),
    }
}

fn validate_listing_binding(
    definition: &InstrumentDefinition,
    listing: &ListingReferenceRecord,
) -> Result<(), MarketProviderConfigurationError> {
    if listing.generation().rights_state() != ListingReferenceRightsState::AdmittedScoped
        || listing.is_test_issue()
        || listing.is_etf() != (definition.asset_class() == AssetClass::Fund)
    {
        return Err(MarketProviderConfigurationError::InvalidListingBinding {
            instrument: definition.instrument_id(),
        });
    }
    let mapping = definition
        .venue_mappings()
        .iter()
        .find(|mapping| mapping.venue_id() == listing.listing_venue())
        .ok_or(MarketProviderConfigurationError::AuthorityRequired {
            instrument: Some(definition.instrument_id()),
            requirement: MarketConfigAuthorityRequirement::InstrumentDefinitionReadCapability,
        })?;
    let symbol = mapping.venue_symbol().as_str();
    if symbol != listing.provider_symbol()
        && listing.cqs_symbol() != Some(symbol)
        && listing.nasdaq_symbol() != Some(symbol)
    {
        return Err(MarketProviderConfigurationError::InvalidListingBinding {
            instrument: definition.instrument_id(),
        });
    }
    Ok(())
}

fn validate_identifier_family(
    definition: &InstrumentDefinition,
    record: &ExternalIdentifierRecord,
) -> Result<(), MarketProviderConfigurationError> {
    let valid = matches!(
        (definition.asset_class(), record.identifier()),
        (AssetClass::Option, ExternalIdentifier::OccOption(_))
            | (AssetClass::Index, ExternalIdentifier::Ticker(_))
            | (AssetClass::Crypto, ExternalIdentifier::CryptoPair(_))
    );
    if !valid {
        return Err(MarketProviderConfigurationError::InvalidIdentifierBinding {
            instrument: definition.instrument_id(),
        });
    }
    Ok(())
}

fn identifier_requirement(class: AssetClass) -> MarketConfigAuthorityRequirement {
    match class {
        AssetClass::Option => MarketConfigAuthorityRequirement::AssignedOccOptionReferenceProducer,
        AssetClass::Index => MarketConfigAuthorityRequirement::AssignedIndexTickerReferenceProducer,
        AssetClass::Crypto => MarketConfigAuthorityRequirement::AssignedCryptoPairReferenceProducer,
        _ => MarketConfigAuthorityRequirement::InstrumentDefinitionReadCapability,
    }
}

fn compare_market_data_bindings(
    left: &MarketDataInstrumentBinding,
    right: &MarketDataInstrumentBinding,
) -> std::cmp::Ordering {
    left.priority
        .cmp(&right.priority)
        .then_with(|| left.instrument_id().cmp(&right.instrument_id()))
        .then_with(|| {
            left.symbol_evidence()
                .source_id()
                .cmp(right.symbol_evidence().source_id())
        })
        .then_with(|| {
            left.provisional_subscription_symbol()
                .cmp(right.provisional_subscription_symbol())
        })
}

fn compare_bindings(
    left: &MarketInstrumentBinding,
    right: &MarketInstrumentBinding,
) -> std::cmp::Ordering {
    left.priority
        .cmp(&right.priority)
        .then_with(|| left.instrument_id().cmp(&right.instrument_id()))
        .then_with(|| left.provider_symbol().cmp(right.provider_symbol()))
}

fn require_distinct_source_ids(
    evidence: &[&MarketSourceEvidence],
) -> Result<(), MarketProviderConfigurationError> {
    for (index, current) in evidence.iter().enumerate() {
        if evidence[..index]
            .iter()
            .any(|prior| prior.source_id == current.source_id)
        {
            return Err(MarketProviderConfigurationError::LogicalSourceConflation);
        }
    }
    Ok(())
}

fn validate_configured_at(
    lease: &ProviderActivationLease,
    configured_at: Timestamp,
) -> Result<(), MarketProviderConfigurationError> {
    let verification_expires_at = lease
        .verification_expires_at()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    if configured_at < lease.issued_at()
        || configured_at < lease.authority_effective_at()
        || configured_at >= verification_expires_at
    {
        return Err(MarketProviderConfigurationError::ConfigurationInstant);
    }
    Ok(())
}

fn validate_source_evidence(
    configured_at: Timestamp,
    evidence: &MarketSourceEvidence,
) -> Result<(), MarketProviderConfigurationError> {
    if !interval_contains(evidence.coverage_effective, configured_at) {
        return Err(MarketProviderConfigurationError::CoverageNotEffective {
            source_id: evidence.source_id.clone(),
        });
    }
    Ok(())
}

fn validate_set_bound<T>(
    surface: &'static str,
    bindings: &[T],
    maximum: usize,
) -> Result<(), MarketProviderConfigurationError> {
    if bindings.is_empty() || bindings.len() > maximum {
        return Err(MarketProviderConfigurationError::InstrumentSetBound { surface, maximum });
    }
    Ok(())
}

fn validate_market_data_bindings(
    configured_at: Timestamp,
    bindings: &[MarketDataInstrumentBinding],
    class_is_admitted: impl Fn(AssetClass) -> bool,
) -> Result<(), MarketProviderConfigurationError> {
    for binding in bindings {
        if !class_is_admitted(binding.asset_class()) {
            return Err(MarketProviderConfigurationError::UnsupportedAssetClass {
                instrument: binding.instrument_id(),
                asset_class: binding.asset_class(),
            });
        }
        if !interval_contains(binding.definition_effective(), configured_at) {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(binding.instrument_id()),
                requirement:
                    MarketConfigAuthorityRequirement::MarketDataInstrumentDefinitionReadCapability,
            });
        }
        if !interval_contains(
            binding.symbol_evidence().effective_interval(),
            configured_at,
        ) {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(binding.instrument_id()),
                requirement:
                    MarketConfigAuthorityRequirement::SourceQualifiedSubscriptionSymbolEvidenceProducer,
            });
        }
    }
    Ok(())
}

fn validate_bindings(
    configured_at: Timestamp,
    bindings: &[MarketInstrumentBinding],
    logical_venue: &'static str,
    class_is_admitted: impl Fn(AssetClass) -> bool,
) -> Result<(), MarketProviderConfigurationError> {
    let venue = VenueId::try_from(logical_venue)?;
    for binding in bindings {
        let definition = binding.definition();
        if !class_is_admitted(definition.asset_class()) {
            return Err(MarketProviderConfigurationError::UnsupportedAssetClass {
                instrument: definition.instrument_id(),
                asset_class: definition.asset_class(),
            });
        }
        if matches!(
            definition.trading_status(),
            TradingStatus::Inactive | TradingStatus::Delisted
        ) {
            return Err(MarketProviderConfigurationError::InactiveInstrument {
                instrument: definition.instrument_id(),
            });
        }
        match &binding.symbol_authority {
            MarketInstrumentSymbolAuthority::AcceptedProviderIdentity {
                provider_identity, ..
            } => {
                let current = definition.provider_identity_at(
                    provider_identity.source_id(),
                    provider_identity.provider_instrument_id(),
                    configured_at,
                );
                if current != Some(provider_identity) {
                    return Err(MarketProviderConfigurationError::AuthorityRequired {
                        instrument: Some(definition.instrument_id()),
                        requirement:
                            MarketConfigAuthorityRequirement::CanonicalProviderIdentityProducer,
                    });
                }
                validate_reference_at(configured_at, binding)?;
            }
            MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional { .. } => {
                if logical_venue != KRAKEN_VENUE || definition.asset_class() != AssetClass::Crypto {
                    return Err(MarketProviderConfigurationError::AuthorityRequired {
                        instrument: Some(definition.instrument_id()),
                        requirement:
                            MarketConfigAuthorityRequirement::CanonicalProviderVenueMappingProducer,
                    });
                }
            }
        }
        if !definition.venue_mappings().iter().any(|mapping| {
            mapping.venue_id() == &venue
                && mapping.venue_symbol().as_str() == binding.provider_symbol()
        }) {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement:
                    MarketConfigAuthorityRequirement::CanonicalProviderVenueMappingProducer,
            });
        }
    }
    Ok(())
}

fn validate_reference_at(
    at: Timestamp,
    binding: &MarketInstrumentBinding,
) -> Result<(), MarketProviderConfigurationError> {
    let Some(MarketInstrumentReferenceBinding::AssignedExternalIdentifier(record)) =
        binding.reference()
    else {
        return Ok(());
    };
    if record.assignment_verification() != AssignmentVerification::VerifiedAssigned
        || record.rights_policy().entitlement() == IdentifierEntitlement::UnknownOrRestricted
        || !interval_contains(record.validity(), at)
    {
        return Err(MarketProviderConfigurationError::AuthorityRequired {
            instrument: Some(binding.instrument_id()),
            requirement: identifier_requirement(binding.definition().asset_class()),
        });
    }
    let exact_symbol = match record.identifier() {
        ExternalIdentifier::OccOption(identity) => {
            let suffix = identity.as_str().get(6..).ok_or(
                MarketProviderConfigurationError::InvalidIdentifierBinding {
                    instrument: binding.instrument_id(),
                },
            )?;
            let mut compact = String::with_capacity(identity.root().len() + suffix.len());
            compact.push_str(identity.root());
            compact.push_str(suffix);
            compact == binding.provider_symbol()
        }
        ExternalIdentifier::Ticker(ticker) => ticker.as_str() == binding.provider_symbol(),
        ExternalIdentifier::CryptoPair(pair) => {
            pair.venue_id().as_str() == KRAKEN_VENUE
                && pair.product_type() == CryptoProductType::Spot
                && pair.raw_product_id().as_str() == binding.provider_symbol()
        }
        _ => false,
    };
    if !exact_symbol {
        return Err(MarketProviderConfigurationError::InvalidIdentifierBinding {
            instrument: binding.instrument_id(),
        });
    }
    Ok(())
}

fn qualified_budget(
    lease: &ProviderActivationLease,
    account: &ProviderAccountBinding,
) -> Result<ProviderBudgetPolicy, MarketProviderConfigurationError> {
    let template = lease
        .provider_budget_policy()
        .cloned()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    Ok(
        ProviderRateDeclaration::try_for_authorization_subject(template, account.subject())?
            .policy()
            .clone(),
    )
}

fn alpaca_historical_research_rights(
    lease: &ProviderActivationLease,
    source_id: &SourceId,
) -> Result<ResearchRightsAuthority, MarketProviderConfigurationError> {
    if !lease.admits(DataUseOperation::Retrieve)
        || !lease.admits(DataUseOperation::Display)
        || !lease.admits(DataUseOperation::Persist)
        || !lease.admits(DataUseOperation::ModelTraining)
        || lease.admits(DataUseOperation::Export)
        || lease.admits(DataUseOperation::Redistribute)
    {
        return Err(MarketProviderConfigurationError::LeaseBinding);
    }
    let evidence = lease
        .persistence_evidence()
        .filter(|evidence| !evidence.refresh_required())
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let basis = RightsBasis::reviewed_terms(
        evidence.official_url(),
        evidence
            .content_digest()
            .ok_or(MarketProviderConfigurationError::LeaseBinding)?,
    )
    .map_err(|_error| MarketProviderConfigurationError::LeaseBinding)?;
    // The settled parent authority permits owner-local private model use, so this child preserves
    // `Train`. Alpaca history still emits locally-first-observed current-research records rather
    // than historical availability/revision evidence; the PIT producer therefore cannot admit it
    // for retrospective training or backtest claims. Evidence eligibility stays separate from
    // the affirmative source-rights record.
    ResearchRightsAuthority::try_new_source_wide(
        source_id.clone(),
        basis,
        lease.rights_decision_digest(),
        lease.verification_expires_at(),
        vec![
            SourceOperation::Retrieve,
            SourceOperation::Display,
            SourceOperation::Persist,
            SourceOperation::Train,
        ],
    )
    .map_err(|_error| MarketProviderConfigurationError::LeaseBinding)
}

fn authorization(
    lease: &ProviderActivationLease,
    account: &ProviderAccountBinding,
) -> Result<AuthorizationGrant, MarketProviderConfigurationError> {
    let expires_at = lease
        .verification_expires_at()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let effective = EffectiveInterval::new(lease.authority_effective_at(), Some(expires_at))?;
    Ok(AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(account.subject().clone()),
        ExactPayloadEvidence::from_content_digest(account.verification_evidence()),
        effective,
    ))
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum MetadataProfile {
    AlpacaIex {
        indicative_options_configured: bool,
        boot_snapshot_revision: &'static str,
        boot_snapshot_maximum_body_bytes: usize,
        boot_snapshot_total_timeout_nanos: u64,
    },
    AlpacaIndicativeOptions,
    AlpacaHistoricalIexDaily,
    KrakenAuthenticatedLevel3 {
        depth: KrakenL3DepthWire,
        tier: KrakenL3ClientTierWire,
    },
}

impl MetadataProfile {
    fn kraken(depth: KrakenL3Depth, tier: KrakenL3ClientTier) -> Self {
        Self::KrakenAuthenticatedLevel3 {
            depth: depth.into(),
            tier: tier.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum KrakenL3DepthWire {
    Ten,
    OneHundred,
    OneThousand,
}

impl From<KrakenL3Depth> for KrakenL3DepthWire {
    fn from(value: KrakenL3Depth) -> Self {
        match value {
            KrakenL3Depth::Ten => Self::Ten,
            KrakenL3Depth::OneHundred => Self::OneHundred,
            KrakenL3Depth::OneThousand => Self::OneThousand,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum KrakenL3ClientTierWire {
    Standard,
    Pro,
}

impl From<KrakenL3ClientTier> for KrakenL3ClientTierWire {
    fn from(value: KrakenL3ClientTier) -> Self {
        match value {
            KrakenL3ClientTier::Standard => Self::Standard,
            KrakenL3ClientTier::Pro => Self::Pro,
        }
    }
}

#[derive(Serialize)]
struct MetadataEvidenceWire<'a, L, B> {
    schema_version: u8,
    configured_at: Timestamp,
    lease: LeaseEvidenceWire<'a>,
    account_subject: &'a SourceIdentifier,
    budget: &'a ProviderBudgetPolicy,
    source: &'a MarketSourceEvidence,
    profile: MetadataProfile,
    limits: &'a L,
    bindings: B,
}

#[derive(Serialize)]
struct LeaseEvidenceWire<'a> {
    session_id: [u8; 16],
    surface_id: &'a SourceIdentifier,
    capability_revision: u64,
    capability_digest: EvidenceDigest,
    rights_decision_digest: EvidenceDigest,
    public_configuration_digest: EvidenceDigest,
    account_digest: EvidenceDigest,
    verification_evidence_digest: EvidenceDigest,
    runtime_evidence_digest: EvidenceDigest,
    generation: u64,
    authority_effective_at: Timestamp,
    verification_expires_at: Timestamp,
    issued_at: Timestamp,
}

#[derive(Serialize)]
struct BindingEvidenceWire<'a> {
    priority: MarketSubscriptionPriority,
    definition: &'a InstrumentDefinition,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_identity: Option<&'a ProviderIdentityRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference: Option<ReferenceEvidenceWire<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kraken_configured_provisional: Option<KrakenConfiguredProvisionalSymbolEvidenceWire<'a>>,
}

#[derive(Serialize)]
struct KrakenConfiguredProvisionalSymbolEvidenceWire<'a> {
    provider_symbol: &'a ProviderInstrumentId,
    venue: &'static str,
    evidence_revision: &'static str,
    runtime_qualification: KrakenRuntimeSymbolQualificationWire,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum KrakenRuntimeSymbolQualificationWire {
    SubscriptionAcknowledgementAndChecksumValidSnapshot,
}

#[derive(Serialize)]
struct MarketDataBindingEvidenceWire<'a> {
    priority: MarketSubscriptionPriority,
    instrument_id: InstrumentId,
    permanent_figi: &'a Figi,
    asset_class: AssetClass,
    definition_reference_evidence: &'a RevisionBoundPayloadEvidence,
    definition_effective: EffectiveInterval,
    definition_revision_digest: EvidenceDigest,
    provisional_subscription_symbol: &'a str,
    symbol_evidence: &'a MarketDataSubscriptionSymbolEvidence,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReferenceEvidenceWire<'a> {
    NasdaqListing {
        source_id: &'a SourceId,
        source_revision: &'a SourceIdentifier,
        source_revision_digest: EvidenceDigest,
        generation_digest: EvidenceDigest,
        generation_sequence: u32,
        rights_id: [u8; 32],
        published_at: Timestamp,
        provider_row_number: u32,
        provider_symbol: &'a str,
        listing_venue: &'a VenueId,
        is_etf: bool,
        is_test_issue: bool,
        round_lot_size: u32,
        record_revision: &'a SourceIdentifier,
        record_payload_evidence: &'a ExactPayloadEvidence,
    },
    AssignedExternalIdentifier {
        record: &'a ExternalIdentifierRecord,
    },
}

impl<'a> From<&'a MarketInstrumentBinding> for BindingEvidenceWire<'a> {
    fn from(binding: &'a MarketInstrumentBinding) -> Self {
        let (provider_identity, reference, kraken_configured_provisional) =
            match &binding.symbol_authority {
            MarketInstrumentSymbolAuthority::AcceptedProviderIdentity {
                provider_identity,
                reference,
            } => {
                let reference = match reference {
                    MarketInstrumentReferenceBinding::NasdaqListing(listing) => {
                        listing_reference_evidence_wire(listing)
                    }
                    MarketInstrumentReferenceBinding::AssignedExternalIdentifier(record) => {
                        ReferenceEvidenceWire::AssignedExternalIdentifier { record }
                    }
                };
                (Some(provider_identity), Some(reference), None)
            }
            MarketInstrumentSymbolAuthority::KrakenConfiguredProvisional { provider_symbol } => {
                (None, None, Some(KrakenConfiguredProvisionalSymbolEvidenceWire {
                    provider_symbol,
                    venue: KRAKEN_VENUE,
                    evidence_revision: KRAKEN_CONFIGURED_SYMBOL_EVIDENCE_REVISION,
                    runtime_qualification:
                        KrakenRuntimeSymbolQualificationWire::SubscriptionAcknowledgementAndChecksumValidSnapshot,
                }))
            }
        };
        Self {
            priority: binding.priority(),
            definition: binding.definition(),
            provider_identity,
            reference,
            kraken_configured_provisional,
        }
    }
}

impl<'a> From<&'a MarketDataInstrumentBinding> for MarketDataBindingEvidenceWire<'a> {
    fn from(binding: &'a MarketDataInstrumentBinding) -> Self {
        Self {
            priority: binding.priority(),
            instrument_id: binding.instrument_id(),
            permanent_figi: binding.permanent_figi(),
            asset_class: binding.asset_class(),
            definition_reference_evidence: binding.definition_reference_evidence(),
            definition_effective: binding.definition_effective(),
            definition_revision_digest: binding.definition_revision_digest(),
            provisional_subscription_symbol: binding.provisional_subscription_symbol(),
            symbol_evidence: binding.symbol_evidence(),
        }
    }
}

fn listing_reference_evidence_wire(listing: &ListingReferenceRecord) -> ReferenceEvidenceWire<'_> {
    let generation = listing.generation();
    ReferenceEvidenceWire::NasdaqListing {
        source_id: generation.source_id(),
        source_revision: generation.source_revision(),
        source_revision_digest: generation.source_revision_digest(),
        generation_digest: generation.generation_digest(),
        generation_sequence: generation.generation_sequence(),
        rights_id: generation.rights_id(),
        published_at: generation.published_at(),
        provider_row_number: listing.provider_row_number(),
        provider_symbol: listing.provider_symbol(),
        listing_venue: listing.listing_venue(),
        is_etf: listing.is_etf(),
        is_test_issue: listing.is_test_issue(),
        round_lot_size: listing.round_lot_size(),
        record_revision: listing.record_revision(),
        record_payload_evidence: listing.record_payload_evidence(),
    }
}

#[derive(Serialize)]
struct AlpacaLimitsWire {
    max_frame_bytes: usize,
    connect_timeout_nanos: u64,
    io_timeout_nanos: u64,
}

impl From<AlpacaTransportLimits> for AlpacaLimitsWire {
    fn from(limits: AlpacaTransportLimits) -> Self {
        Self {
            max_frame_bytes: limits.max_frame_bytes(),
            connect_timeout_nanos: duration_nanos_saturating(limits.connect_timeout()),
            io_timeout_nanos: duration_nanos_saturating(limits.io_timeout()),
        }
    }
}

#[derive(Serialize)]
struct KrakenLimitsWire {
    max_message_bytes: usize,
}

fn duration_nanos_saturating(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn metadata_digest<L: Serialize>(
    lease: &ProviderActivationLease,
    configured_at: Timestamp,
    account: &ProviderAccountBinding,
    budget: &ProviderBudgetPolicy,
    source: &MarketSourceEvidence,
    profile: MetadataProfile,
    bindings: &[MarketInstrumentBinding],
    limits: &L,
) -> Result<EvidenceDigest, MarketProviderConfigurationError> {
    metadata_digest_for_bindings(
        METADATA_EVIDENCE_DOMAIN,
        lease,
        configured_at,
        account,
        budget,
        source,
        profile,
        bindings
            .iter()
            .map(BindingEvidenceWire::from)
            .collect::<Vec<_>>(),
        limits,
    )
}

fn display_metadata_digest<L: Serialize>(
    lease: &ProviderActivationLease,
    configured_at: Timestamp,
    account: &ProviderAccountBinding,
    budget: &ProviderBudgetPolicy,
    source: &MarketSourceEvidence,
    profile: MetadataProfile,
    bindings: &[MarketDataInstrumentBinding],
    limits: &L,
) -> Result<EvidenceDigest, MarketProviderConfigurationError> {
    metadata_digest_for_bindings(
        DISPLAY_METADATA_EVIDENCE_DOMAIN,
        lease,
        configured_at,
        account,
        budget,
        source,
        profile,
        bindings
            .iter()
            .map(MarketDataBindingEvidenceWire::from)
            .collect::<Vec<_>>(),
        limits,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the evidence envelope keeps every authority input explicit"
)]
fn metadata_digest_for_bindings<L: Serialize, B: Serialize>(
    evidence_domain: &[u8],
    lease: &ProviderActivationLease,
    configured_at: Timestamp,
    account: &ProviderAccountBinding,
    budget: &ProviderBudgetPolicy,
    source: &MarketSourceEvidence,
    profile: MetadataProfile,
    bindings: B,
    limits: &L,
) -> Result<EvidenceDigest, MarketProviderConfigurationError> {
    let account_digest = lease
        .account_digest()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let verification = lease
        .verification_evidence_digest()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let generation = lease
        .generation()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let expires_at = lease
        .verification_expires_at()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let wire = MetadataEvidenceWire {
        schema_version: 2,
        configured_at,
        lease: LeaseEvidenceWire {
            session_id: *lease.session_id().as_bytes(),
            surface_id: lease.surface_id(),
            capability_revision: lease.capability_revision().get(),
            capability_digest: lease.capability_digest(),
            rights_decision_digest: lease.rights_decision_digest(),
            public_configuration_digest: lease.public_configuration_digest(),
            account_digest,
            verification_evidence_digest: verification,
            runtime_evidence_digest: lease.runtime_evidence_digest(),
            generation: generation.get(),
            authority_effective_at: lease.authority_effective_at(),
            verification_expires_at: expires_at,
            issued_at: lease.issued_at(),
        },
        account_subject: account.subject(),
        budget,
        source,
        profile,
        limits,
        bindings,
    };
    let canonical = serde_json::to_vec(&wire)
        .map_err(|_error| MarketProviderConfigurationError::EvidenceEncoding)?;
    let mut hasher = Sha256::new();
    hasher.update(evidence_domain);
    hasher.update(
        u64::try_from(canonical.len())
            .map_err(|_error| MarketProviderConfigurationError::EvidenceEncoding)?
            .to_be_bytes(),
    );
    hasher.update(canonical);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn revision_evidence(
    lease: &ProviderActivationLease,
    prefix: &'static str,
    digest: EvidenceDigest,
) -> Result<RevisionBoundPayloadEvidence, MarketProviderConfigurationError> {
    let generation = lease
        .generation()
        .ok_or(MarketProviderConfigurationError::LeaseBinding)?;
    let revision = SourceIdentifier::try_from(format!(
        "{prefix}-r{}-g{}-{}",
        lease.capability_revision().get(),
        generation.get(),
        short_hex(digest.bytes()),
    ))?;
    Ok(RevisionBoundPayloadEvidence::new(
        MetadataRevision::new(revision),
        ExactPayloadEvidence::from_content_digest(digest),
    ))
}

fn short_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(24);
    for byte in bytes.into_iter().take(12) {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at && interval.ends_at().is_none_or(|end| at < end)
}

/// Fail-closed market-provider configuration failure.
#[derive(Debug, Error)]
pub enum MarketProviderConfigurationError {
    /// A required canonical producer has not supplied sufficient security-level authority.
    #[error("market-provider configuration requires {requirement} for instrument {instrument:?}")]
    AuthorityRequired {
        instrument: Option<InstrumentId>,
        requirement: MarketConfigAuthorityRequirement,
    },
    /// A provider surface received an empty or oversized priority set.
    #[error("{surface} instrument set must contain between 1 and {maximum} unique bindings")]
    InstrumentSetBound {
        surface: &'static str,
        maximum: usize,
    },
    /// Optional source evidence and its nonempty instrument set must be present together.
    #[error("{surface} evidence and bounded instrument set must both be present or both be absent")]
    OptionalCapabilityBinding { surface: &'static str },
    /// One stable ID or provider symbol appeared twice in a logical source set.
    #[error("market-provider instrument binding is duplicated")]
    DuplicateInstrumentBinding,
    /// Two access/logical surfaces attempted to share one source metadata identity.
    #[error("distinct market-provider logical/access surfaces require distinct SourceId values")]
    LogicalSourceConflation,
    /// Explicit coverage evidence was not effective at the configuration instant.
    #[error("market-provider coverage evidence for {source_id} is not effective at configuration")]
    CoverageNotEffective { source_id: SourceId },
    /// Nasdaq listing evidence did not corroborate the supplied stable identity authority.
    #[error("Nasdaq listing evidence does not corroborate stable instrument {instrument}")]
    InvalidListingBinding { instrument: InstrumentId },
    /// An assigned external identifier did not exactly describe the provider symbol/family.
    #[error("assigned external identifier does not match canonical instrument {instrument}")]
    InvalidIdentifierBinding { instrument: InstrumentId },
    /// The adapter profile does not admit the canonical asset family.
    #[error("market-provider profile does not admit {asset_class:?} instrument {instrument}")]
    UnsupportedAssetClass {
        instrument: InstrumentId,
        asset_class: AssetClass,
    },
    /// Inactive/delisted canonical instruments cannot enter a live subscription config.
    #[error("canonical instrument {instrument} is inactive or delisted")]
    InactiveInstrument { instrument: InstrumentId },
    /// The lease omitted one required account/generation/budget fact.
    #[error("active provider lease is missing an authenticated market-data binding")]
    LeaseBinding,
    /// The caller-supplied configuration instant is outside the active verified lease interval.
    #[error("market-provider configuration instant is outside active verified authority")]
    ConfigurationInstant,
    /// Deterministic metadata evidence could not be encoded.
    #[error("market-provider metadata evidence could not be encoded deterministically")]
    EvidenceEncoding,
    /// The exact active onboarding lease is unavailable or stale.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    /// Account identity or lease-surface validation failed.
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    /// A bounded domain identity was invalid.
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// An effective interval was invalid.
    #[error("market-provider effective interval is invalid")]
    EffectiveInterval(#[from] market_squawk_domain::InstrumentError),
    /// Account-qualified provider-budget construction failed.
    #[error(transparent)]
    Budget(#[from] BudgetPoolError),
    /// Alpaca rejected the bounded mapping or metadata.
    #[error(transparent)]
    Alpaca(#[from] AlpacaError),
    /// Kraken rejected the bounded authenticated level-3 configuration.
    #[error(transparent)]
    KrakenConfig(#[from] KrakenL3ConfigError),
    /// Kraken rejected authenticated level-3 metadata.
    #[error(transparent)]
    KrakenMetadata(#[from] KrakenL3MetadataError),
}
