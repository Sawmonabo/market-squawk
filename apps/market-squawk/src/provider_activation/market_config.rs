//! Evidence-bound construction of authenticated market-provider configurations.
//!
//! This boundary deliberately does not discover instruments, read the network, or mint stable
//! instrument identities. Callers must first resolve every requested instrument through the
//! canonical instrument-definition authority and, where applicable, bind it to either a
//! catalog-minted Nasdaq listing-reference row or an assigned external-identifier record already
//! retained by that definition. A Nasdaq directory row or SEC company record is therefore never
//! sufficient input for a tradable [`InstrumentId`](market_squawk_domain::InstrumentId).

use std::num::NonZeroUsize;

use market_squawk_adapter_alpaca::{
    ALPACA_BASIC_EQUITY_SYMBOL_LIMIT, ALPACA_BASIC_OPTION_SYMBOL_LIMIT, AlpacaError,
    AlpacaIexLiveConfig, AlpacaInstrumentMapping, AlpacaOptionMapping, AlpacaOptionsLiveConfig,
    AlpacaTransportLimits,
};
use market_squawk_adapter_kraken::{
    KrakenL3ClientTier, KrakenL3Config, KrakenL3ConfigError, KrakenL3Depth, KrakenL3MetadataError,
    KrakenL3MetadataInput, KrakenL3ProductMapping,
};
use market_squawk_adapter_tradier::{
    TradierAccessSurface, TradierConfigError, TradierInstrumentKind, TradierLogicalProfile,
    TradierSourceConfig, TradierSymbolMapping, TradierTransportLimits,
};
use market_squawk_data::{ListingReferenceRecord, ListingReferenceRightsState};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, AuthorizationBasis, CryptoProductType, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier,
    ExternalIdentifierRecord, IdentifierEntitlement, InstrumentDefinition,
    InstrumentExecutionTerms, InstrumentId, MetadataRevision, ProviderIdentityRecord,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp, TradingStatus, VenueId,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BudgetPoolError, FreshnessPolicy, ProviderBudgetPolicy,
    ProviderRateDeclaration,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::ProviderAdapterActivation;
use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderMarketAccount,
};

const MAX_PRIORITY_BINDINGS: usize = 256;
const TRADIER_CONSOLIDATED_SYMBOL_LIMIT: usize = 256;
const TRADIER_DERIVED_INDEX_SYMBOL_LIMIT: usize = 3;
const KRAKEN_L3_PRODUCT_LIMIT: usize = 200;

const ALPACA_IEX_VENUE: &str = "iex";
const ALPACA_OPTIONS_VENUE: &str = "alpaca-indicative-options";
const TRADIER_CONSOLIDATED_VENUE: &str = "tradier-consolidated-us";
const TRADIER_DERIVED_INDEX_VENUE: &str = "tradier-derived-index";
const KRAKEN_VENUE: &str = "kraken";

const METADATA_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/authenticated-market-provider-metadata/v1\0";

/// Exact producer required when a security-level provider mapping is unavailable.
///
/// These requirements are intentionally narrower than Nasdaq listing-directory or SEC-company
/// authority. Neither reference surface owns stable instrument identity, provider symbol
/// crosswalks, or exact tick/lot/multiplier terms.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketConfigAuthorityRequirement {
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

/// One explicit provider-symbol/canonical-instrument/reference binding.
///
/// The stable instrument ID and exact execution terms always come from `definition`. The provider
/// symbol always comes from `provider_identity`. `reference` can corroborate the mapping, but it
/// cannot create or replace either authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketInstrumentBinding {
    priority: MarketSubscriptionPriority,
    definition: InstrumentDefinition,
    provider_identity: ProviderIdentityRecord,
    reference: MarketInstrumentReferenceBinding,
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
            provider_identity,
            reference,
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

    /// Returns the exact accepted provider-symbol assertion.
    pub const fn provider_identity(&self) -> &ProviderIdentityRecord {
        &self.provider_identity
    }

    /// Returns the explicit listing or assigned-identifier corroboration.
    pub const fn reference(&self) -> &MarketInstrumentReferenceBinding {
        &self.reference
    }

    /// Returns the stable instrument ID supplied by the canonical definition.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.definition.instrument_id()
    }

    /// Returns the exact revision-bound terms required by order-level normalization.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.definition.execution_terms()
    }

    /// Returns the source-native symbol supplied by accepted provider identity evidence.
    pub fn provider_symbol(&self) -> &str {
        self.provider_identity.provider_instrument_id().as_str()
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
    /// order is priority, stable instrument ID, provider-identity source, then provider symbol.
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
    pub iex_evidence: MarketSourceEvidence,
    pub options_evidence: MarketSourceEvidence,
    pub iex_instruments: BoundedMarketInstrumentSet,
    pub option_instruments: BoundedMarketInstrumentSet,
    pub transport_limits: AlpacaTransportLimits,
}

/// Explicit Tradier construction inputs for three distinct logical/access surfaces.
#[derive(Clone, Debug)]
pub struct TradierMarketConfigurationInput {
    pub consolidated_stream_evidence: MarketSourceEvidence,
    pub consolidated_rest_evidence: MarketSourceEvidence,
    pub derived_index_rest_evidence: MarketSourceEvidence,
    pub consolidated_instruments: BoundedMarketInstrumentSet,
    pub derived_indexes: BoundedMarketInstrumentSet,
    pub transport_limits: TradierTransportLimits,
}

/// Explicit authenticated Kraken level-3 construction inputs.
#[derive(Clone, Debug)]
pub struct KrakenL3MarketConfigurationInput {
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
    Tradier(TradierMarketConfigurationInput),
    KrakenLevel3(KrakenL3MarketConfigurationInput),
}

/// Prepared Alpaca configs plus the exact lease and canonical definitions that produced them.
#[derive(Clone, Debug)]
pub struct PreparedAlpacaBasicMarketConfiguration {
    lease: ProviderActivationLease,
    account: ProviderAccountBinding,
    iex: AlpacaIexLiveConfig,
    options: AlpacaOptionsLiveConfig,
    iex_instruments: Box<[MarketInstrumentBinding]>,
    option_instruments: Box<[MarketInstrumentBinding]>,
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

    pub const fn options_config(&self) -> &AlpacaOptionsLiveConfig {
        &self.options
    }

    pub fn iex_instruments(&self) -> &[MarketInstrumentBinding] {
        &self.iex_instruments
    }

    pub fn option_instruments(&self) -> &[MarketInstrumentBinding] {
        &self.option_instruments
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
        AlpacaOptionsLiveConfig,
        Box<[MarketInstrumentBinding]>,
        Box<[MarketInstrumentBinding]>,
    ) {
        (
            self.lease,
            self.iex,
            self.options,
            self.iex_instruments,
            self.option_instruments,
        )
    }
}

/// Prepared Tradier configs plus the exact lease and separately retained source bindings.
#[derive(Clone, Debug)]
pub struct PreparedTradierMarketConfiguration {
    lease: ProviderActivationLease,
    account: ProviderAccountBinding,
    consolidated_stream: TradierSourceConfig,
    consolidated_rest: TradierSourceConfig,
    derived_index_rest: TradierSourceConfig,
    consolidated_instruments: Box<[MarketInstrumentBinding]>,
    derived_indexes: Box<[MarketInstrumentBinding]>,
}

impl PreparedTradierMarketConfiguration {
    pub const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    pub const fn account_binding(&self) -> &ProviderAccountBinding {
        &self.account
    }

    pub const fn consolidated_stream_config(&self) -> &TradierSourceConfig {
        &self.consolidated_stream
    }

    pub const fn consolidated_rest_config(&self) -> &TradierSourceConfig {
        &self.consolidated_rest
    }

    pub const fn derived_index_rest_config(&self) -> &TradierSourceConfig {
        &self.derived_index_rest
    }

    pub fn consolidated_instruments(&self) -> &[MarketInstrumentBinding] {
        &self.consolidated_instruments
    }

    pub fn derived_indexes(&self) -> &[MarketInstrumentBinding] {
        &self.derived_indexes
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
        TradierSourceConfig,
        TradierSourceConfig,
        TradierSourceConfig,
        Box<[MarketInstrumentBinding]>,
        Box<[MarketInstrumentBinding]>,
    ) {
        (
            self.lease,
            self.consolidated_stream,
            self.consolidated_rest,
            self.derived_index_rest,
            self.consolidated_instruments,
            self.derived_indexes,
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
    Tradier(PreparedTradierMarketConfiguration),
    KrakenLevel3(PreparedKrakenL3MarketConfiguration),
}

impl ProviderAdapterActivation {
    /// Constructs adapter configurations while holding exact active-lease mutation authority.
    ///
    /// This method performs no network access, reads no credential, and retains no secret. It
    /// validates the exact active lease before and after construction while the onboarding
    /// mutation guard is held. Every metadata revision is a deterministic SHA-256 commitment to
    /// the lease, qualified account budget, logical source evidence, provider profile, transport
    /// bounds, and sorted canonical instrument bindings.
    ///
    /// # Errors
    ///
    /// Fails closed for a stale/mismatched lease, missing canonical security-level authority,
    /// stale or quarantined provider identity, incompatible listing/identifier evidence,
    /// duplicated/unbounded priority set, logical-source conflation, or adapter contract failure.
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
            ProviderMarketConfigurationRequest::Tradier(input) => {
                PreparedMarketProviderConfiguration::Tradier(prepare_tradier(&lease, input)?)
            }
            ProviderMarketConfigurationRequest::KrakenLevel3(input) => {
                PreparedMarketProviderConfiguration::KrakenLevel3(prepare_kraken_l3(&lease, input)?)
            }
        };
        authority.require_active(&lease)?;
        Ok(configured)
    }
}

fn prepare_alpaca(
    lease: &ProviderActivationLease,
    input: AlpacaBasicMarketConfigurationInput,
) -> Result<PreparedAlpacaBasicMarketConfiguration, MarketProviderConfigurationError> {
    let account =
        ProviderAccountBinding::try_from_lease(ProviderMarketAccount::AlpacaBasic, lease)?;
    require_distinct_source_ids(&[&input.iex_evidence, &input.options_evidence])?;
    validate_source_evidence(lease, &input.iex_evidence)?;
    validate_source_evidence(lease, &input.options_evidence)?;
    validate_set_bound(
        "alpaca-basic-iex",
        input.iex_instruments.bindings(),
        ALPACA_BASIC_EQUITY_SYMBOL_LIMIT,
    )?;
    validate_set_bound(
        "alpaca-basic-indicative-options",
        input.option_instruments.bindings(),
        ALPACA_BASIC_OPTION_SYMBOL_LIMIT,
    )?;
    validate_bindings(
        lease,
        input.iex_instruments.bindings(),
        ALPACA_IEX_VENUE,
        |class| matches!(class, AssetClass::Equity | AssetClass::Fund),
    )?;
    validate_bindings(
        lease,
        input.option_instruments.bindings(),
        ALPACA_OPTIONS_VENUE,
        |class| class == AssetClass::Option,
    )?;
    let budget = qualified_budget(lease, &account)?;
    let authorization = authorization(lease, &account)?;
    let iex_digest = metadata_digest(
        lease,
        &account,
        &budget,
        &input.iex_evidence,
        MetadataProfile::AlpacaIex,
        input.iex_instruments.bindings(),
        &AlpacaLimitsWire::from(input.transport_limits),
    )?;
    let options_digest = metadata_digest(
        lease,
        &account,
        &budget,
        &input.options_evidence,
        MetadataProfile::AlpacaIndicativeOptions,
        input.option_instruments.bindings(),
        &AlpacaLimitsWire::from(input.transport_limits),
    )?;
    let iex_mappings = input
        .iex_instruments
        .bindings()
        .iter()
        .map(|binding| {
            AlpacaInstrumentMapping::try_new(
                binding.provider_symbol().to_owned(),
                binding.instrument_id(),
                binding.definition().asset_class(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let option_mappings = input
        .option_instruments
        .bindings()
        .iter()
        .map(|binding| {
            AlpacaOptionMapping::try_new(
                binding.provider_symbol().to_owned(),
                binding.instrument_id(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let iex = AlpacaIexLiveConfig::try_new(
        input.iex_evidence.source_id.clone(),
        revision_evidence(lease, "alpaca-iex", iex_digest)?,
        authorization.clone(),
        input.iex_evidence.coverage_evidence.clone(),
        input.iex_evidence.coverage_effective,
        iex_mappings,
        input.iex_evidence.freshness,
        budget.clone(),
        input.transport_limits,
    )?;
    let options = AlpacaOptionsLiveConfig::try_new(
        input.options_evidence.source_id.clone(),
        revision_evidence(lease, "alpaca-options", options_digest)?,
        authorization,
        input.options_evidence.coverage_evidence.clone(),
        input.options_evidence.coverage_effective,
        option_mappings,
        input.options_evidence.freshness,
        budget,
        input.transport_limits,
    )?;
    Ok(PreparedAlpacaBasicMarketConfiguration {
        lease: lease.clone(),
        account,
        iex,
        options,
        iex_instruments: input.iex_instruments.into_bindings().into_boxed_slice(),
        option_instruments: input.option_instruments.into_bindings().into_boxed_slice(),
    })
}

fn prepare_tradier(
    lease: &ProviderActivationLease,
    input: TradierMarketConfigurationInput,
) -> Result<PreparedTradierMarketConfiguration, MarketProviderConfigurationError> {
    let account =
        ProviderAccountBinding::try_from_lease(ProviderMarketAccount::TradierBrokerage, lease)?;
    require_distinct_source_ids(&[
        &input.consolidated_stream_evidence,
        &input.consolidated_rest_evidence,
        &input.derived_index_rest_evidence,
    ])?;
    for evidence in [
        &input.consolidated_stream_evidence,
        &input.consolidated_rest_evidence,
        &input.derived_index_rest_evidence,
    ] {
        validate_source_evidence(lease, evidence)?;
    }
    validate_set_bound(
        "tradier-consolidated-securities",
        input.consolidated_instruments.bindings(),
        TRADIER_CONSOLIDATED_SYMBOL_LIMIT,
    )?;
    validate_set_bound(
        "tradier-derived-indexes",
        input.derived_indexes.bindings(),
        TRADIER_DERIVED_INDEX_SYMBOL_LIMIT,
    )?;
    validate_bindings(
        lease,
        input.consolidated_instruments.bindings(),
        TRADIER_CONSOLIDATED_VENUE,
        |class| {
            matches!(
                class,
                AssetClass::Equity | AssetClass::Fund | AssetClass::Option
            )
        },
    )?;
    validate_bindings(
        lease,
        input.derived_indexes.bindings(),
        TRADIER_DERIVED_INDEX_VENUE,
        |class| class == AssetClass::Index,
    )?;
    let budget = qualified_budget(lease, &account)?;
    let authorization = authorization(lease, &account)?;
    let mappings = input
        .consolidated_instruments
        .bindings()
        .iter()
        .map(tradier_mapping)
        .collect::<Result<Vec<_>, _>>()?;
    let index_mappings = input
        .derived_indexes
        .bindings()
        .iter()
        .map(tradier_mapping)
        .collect::<Result<Vec<_>, _>>()?;
    let limits_wire = TradierLimitsWire::from(input.transport_limits);
    let stream_digest = metadata_digest(
        lease,
        &account,
        &budget,
        &input.consolidated_stream_evidence,
        MetadataProfile::TradierConsolidatedStream,
        input.consolidated_instruments.bindings(),
        &limits_wire,
    )?;
    let rest_digest = metadata_digest(
        lease,
        &account,
        &budget,
        &input.consolidated_rest_evidence,
        MetadataProfile::TradierConsolidatedRest,
        input.consolidated_instruments.bindings(),
        &limits_wire,
    )?;
    let index_digest = metadata_digest(
        lease,
        &account,
        &budget,
        &input.derived_index_rest_evidence,
        MetadataProfile::TradierDerivedIndexRest,
        input.derived_indexes.bindings(),
        &limits_wire,
    )?;
    let consolidated_stream = TradierSourceConfig::try_new(
        input.consolidated_stream_evidence.source_id.clone(),
        revision_evidence(lease, "tradier-stream", stream_digest)?,
        authorization.clone(),
        input.consolidated_stream_evidence.coverage_evidence.clone(),
        input.consolidated_stream_evidence.coverage_effective,
        TradierLogicalProfile::ConsolidatedSecurities,
        TradierAccessSurface::Streaming,
        mappings.clone(),
        input.consolidated_stream_evidence.freshness,
        budget.clone(),
        input.transport_limits,
    )?;
    let consolidated_rest = TradierSourceConfig::try_new(
        input.consolidated_rest_evidence.source_id.clone(),
        revision_evidence(lease, "tradier-rest", rest_digest)?,
        authorization.clone(),
        input.consolidated_rest_evidence.coverage_evidence.clone(),
        input.consolidated_rest_evidence.coverage_effective,
        TradierLogicalProfile::ConsolidatedSecurities,
        TradierAccessSurface::RestSnapshots,
        mappings,
        input.consolidated_rest_evidence.freshness,
        budget.clone(),
        input.transport_limits,
    )?;
    let derived_index_rest = TradierSourceConfig::try_new(
        input.derived_index_rest_evidence.source_id.clone(),
        revision_evidence(lease, "tradier-index", index_digest)?,
        authorization,
        input.derived_index_rest_evidence.coverage_evidence.clone(),
        input.derived_index_rest_evidence.coverage_effective,
        TradierLogicalProfile::DerivedIndexes,
        TradierAccessSurface::RestSnapshots,
        index_mappings,
        input.derived_index_rest_evidence.freshness,
        budget,
        input.transport_limits,
    )?;
    Ok(PreparedTradierMarketConfiguration {
        lease: lease.clone(),
        account,
        consolidated_stream,
        consolidated_rest,
        derived_index_rest,
        consolidated_instruments: input
            .consolidated_instruments
            .into_bindings()
            .into_boxed_slice(),
        derived_indexes: input.derived_indexes.into_bindings().into_boxed_slice(),
    })
}

fn prepare_kraken_l3(
    lease: &ProviderActivationLease,
    input: KrakenL3MarketConfigurationInput,
) -> Result<PreparedKrakenL3MarketConfiguration, MarketProviderConfigurationError> {
    let account =
        ProviderAccountBinding::try_from_lease(ProviderMarketAccount::KrakenLevel3, lease)?;
    validate_source_evidence(lease, &input.evidence)?;
    validate_set_bound(
        "kraken-authenticated-level3",
        input.instruments.bindings(),
        KRAKEN_L3_PRODUCT_LIMIT,
    )?;
    validate_bindings(lease, input.instruments.bindings(), KRAKEN_VENUE, |class| {
        class == AssetClass::Crypto
    })?;
    let budget = qualified_budget(lease, &account)?;
    let authorization = authorization(lease, &account)?;
    let digest = metadata_digest(
        lease,
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

fn compare_bindings(
    left: &MarketInstrumentBinding,
    right: &MarketInstrumentBinding,
) -> std::cmp::Ordering {
    left.priority
        .cmp(&right.priority)
        .then_with(|| left.instrument_id().cmp(&right.instrument_id()))
        .then_with(|| {
            left.provider_identity
                .source_id()
                .cmp(right.provider_identity.source_id())
        })
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

fn validate_source_evidence(
    lease: &ProviderActivationLease,
    evidence: &MarketSourceEvidence,
) -> Result<(), MarketProviderConfigurationError> {
    if !interval_contains(evidence.coverage_effective, lease.issued_at()) {
        return Err(MarketProviderConfigurationError::CoverageNotEffective {
            source_id: evidence.source_id.clone(),
        });
    }
    Ok(())
}

fn validate_set_bound(
    surface: &'static str,
    bindings: &[MarketInstrumentBinding],
    maximum: usize,
) -> Result<(), MarketProviderConfigurationError> {
    if bindings.is_empty() || bindings.len() > maximum {
        return Err(MarketProviderConfigurationError::InstrumentSetBound { surface, maximum });
    }
    Ok(())
}

fn validate_bindings(
    lease: &ProviderActivationLease,
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
        let provider_identity = binding.provider_identity();
        let current = definition.provider_identity_at(
            provider_identity.source_id(),
            provider_identity.provider_instrument_id(),
            lease.issued_at(),
        );
        if current != Some(provider_identity) {
            return Err(MarketProviderConfigurationError::AuthorityRequired {
                instrument: Some(definition.instrument_id()),
                requirement: MarketConfigAuthorityRequirement::CanonicalProviderIdentityProducer,
            });
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
        validate_reference_at(lease.issued_at(), binding)?;
    }
    Ok(())
}

fn validate_reference_at(
    at: Timestamp,
    binding: &MarketInstrumentBinding,
) -> Result<(), MarketProviderConfigurationError> {
    let MarketInstrumentReferenceBinding::AssignedExternalIdentifier(record) = binding.reference()
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

fn tradier_mapping(
    binding: &MarketInstrumentBinding,
) -> Result<TradierSymbolMapping, MarketProviderConfigurationError> {
    let kind = match binding.definition().asset_class() {
        AssetClass::Equity => TradierInstrumentKind::Equity,
        AssetClass::Fund => TradierInstrumentKind::Etf,
        AssetClass::Option => TradierInstrumentKind::Option,
        AssetClass::Index => TradierInstrumentKind::DerivedIndex,
        class => {
            return Err(MarketProviderConfigurationError::UnsupportedAssetClass {
                instrument: binding.instrument_id(),
                asset_class: class,
            });
        }
    };
    Ok(TradierSymbolMapping::try_new(
        SourceIdentifier::try_from(binding.provider_symbol())?,
        binding.instrument_id(),
        kind,
    )?)
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
    AlpacaIex,
    AlpacaIndicativeOptions,
    TradierConsolidatedStream,
    TradierConsolidatedRest,
    TradierDerivedIndexRest,
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
struct MetadataEvidenceWire<'a, L: Serialize> {
    schema_version: u8,
    lease: LeaseEvidenceWire<'a>,
    account_subject: &'a SourceIdentifier,
    budget: &'a ProviderBudgetPolicy,
    source: &'a MarketSourceEvidence,
    profile: MetadataProfile,
    limits: &'a L,
    bindings: Vec<BindingEvidenceWire<'a>>,
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
    provider_identity: &'a ProviderIdentityRecord,
    reference: ReferenceEvidenceWire<'a>,
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
        let reference = match binding.reference() {
            MarketInstrumentReferenceBinding::NasdaqListing(listing) => {
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
            MarketInstrumentReferenceBinding::AssignedExternalIdentifier(record) => {
                ReferenceEvidenceWire::AssignedExternalIdentifier { record }
            }
        };
        Self {
            priority: binding.priority(),
            definition: binding.definition(),
            provider_identity: binding.provider_identity(),
            reference,
        }
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
struct TradierLimitsWire {
    max_frame_bytes: usize,
    io_timeout_nanos: u64,
    connect_timeout_nanos: u64,
    read_timeout_nanos: u64,
    total_timeout_nanos: u64,
    max_redirects: u8,
    max_response_bytes: u64,
}

impl From<TradierTransportLimits> for TradierLimitsWire {
    fn from(limits: TradierTransportLimits) -> Self {
        let http = limits.http();
        Self {
            max_frame_bytes: limits.max_frame_bytes(),
            io_timeout_nanos: duration_nanos_saturating(limits.io_timeout()),
            connect_timeout_nanos: http.connect_timeout_nanos(),
            read_timeout_nanos: http.read_timeout_nanos(),
            total_timeout_nanos: http.total_timeout_nanos(),
            max_redirects: http.max_redirects(),
            max_response_bytes: http.max_response_bytes(),
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
    account: &ProviderAccountBinding,
    budget: &ProviderBudgetPolicy,
    source: &MarketSourceEvidence,
    profile: MetadataProfile,
    bindings: &[MarketInstrumentBinding],
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
        schema_version: 1,
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
        bindings: bindings.iter().map(BindingEvidenceWire::from).collect(),
    };
    let canonical = serde_json::to_vec(&wire)
        .map_err(|_error| MarketProviderConfigurationError::EvidenceEncoding)?;
    let mut hasher = Sha256::new();
    hasher.update(METADATA_EVIDENCE_DOMAIN);
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
    #[error(
        "market-provider configuration requires {requirement} for instrument {instrument:?}; a Nasdaq directory row or SEC company record cannot supply it"
    )]
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
    /// One stable ID or provider symbol appeared twice in a logical source set.
    #[error("market-provider instrument binding is duplicated")]
    DuplicateInstrumentBinding,
    /// Two access/logical surfaces attempted to share one source metadata identity.
    #[error("distinct market-provider logical/access surfaces require distinct SourceId values")]
    LogicalSourceConflation,
    /// Explicit coverage evidence was not effective when the exact lease was issued.
    #[error("market-provider coverage evidence for {source_id} is not effective for this lease")]
    CoverageNotEffective { source_id: SourceId },
    /// A listed-security row did not corroborate the canonical listing mapping.
    #[error("Nasdaq listing reference does not corroborate canonical instrument {instrument}")]
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
    /// Tradier rejected the bounded mapping or metadata.
    #[error(transparent)]
    Tradier(#[from] TradierConfigError),
    /// Kraken rejected the bounded authenticated level-3 configuration.
    #[error(transparent)]
    KrakenConfig(#[from] KrakenL3ConfigError),
    /// Kraken rejected authenticated level-3 metadata.
    #[error(transparent)]
    KrakenMetadata(#[from] KrakenL3MetadataError),
}
