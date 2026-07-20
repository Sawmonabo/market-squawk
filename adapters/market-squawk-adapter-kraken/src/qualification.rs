//! Reviewed qualification decision for Kraken Spot WebSocket v2.

use std::num::NonZeroU16;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, ExecutionEligibility, InstrumentId, IntegrityRule,
    LiveEventClass, MarketDepth, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence,
    RuleVersion, SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId,
    SourceIdentifier, VenueId,
};
use market_squawk_live::{KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID};
use market_squawk_sources::{
    AuthorizationGrant, ChecksumAlgorithm, ChecksumBookScope, ChecksumValidationProfile,
    CoverageTopology, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataError, SourceMetadataInput, SourceProtocolProfile,
};
use thiserror::Error;

use crate::KrakenChannel;

/// Reviewed policy revision. Increment only with a new evidence review.
pub const KRAKEN_QUALIFICATION_POLICY_VERSION: u32 = 1;
/// SHA-256 of the policy decision record used by the adapter release.
pub const KRAKEN_QUALIFICATION_POLICY_DIGEST: &str =
    "23f1d350faa0dbfb95f3521f43b0b200ac34e680c8b6c8d346da95c91be12f5b";
/// Provider rule identity: the book protocol authoritatively supplies no sequence field.
pub const KRAKEN_BOOK_SEQUENCE_RULE: &str = "kraken-ws-v2-book-sequence-unsupported-policy-v1-sha256-23f1d350faa0dbfb95f3521f43b0b200ac34e680c8b6c8d346da95c91be12f5b";
/// Trade IDs identify trades but are not promoted to a channel-sequence capability.
pub const KRAKEN_TRADE_SEQUENCE_RULE: &str = "kraken-ws-v2-trade-channel-sequence-unsupported-policy-v1-sha256-23f1d350faa0dbfb95f3521f43b0b200ac34e680c8b6c8d346da95c91be12f5b";

/// Caller-owned evidence needed to construct truthful immutable Kraken source metadata.
#[derive(Clone, Debug)]
pub struct KrakenMetadataInput {
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    authorization: AuthorizationGrant,
    coverage_evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    instrument: InstrumentId,
    freshness: FreshnessPolicy,
    budget: ProviderBudgetPolicy,
    channel: KrakenChannel,
}

impl KrakenMetadataInput {
    /// Collects independent source-policy and rights evidence.
    #[allow(
        clippy::too_many_arguments,
        reason = "source identity, rights, coverage, timing, and budget evidence stay explicit"
    )]
    pub const fn new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        instrument: InstrumentId,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
    ) -> Self {
        Self::new_for_channel(
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            instrument,
            freshness,
            budget,
            KrakenChannel::Book(crate::KrakenDepth::Ten),
        )
    }

    /// Collects evidence for an independently registered trade channel.
    #[allow(
        clippy::too_many_arguments,
        reason = "source identity, rights, coverage, timing, and budget evidence stay explicit"
    )]
    pub const fn new_trades(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        instrument: InstrumentId,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
    ) -> Self {
        Self::new_for_channel(
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            instrument,
            freshness,
            budget,
            KrakenChannel::Trades,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private common constructor keeps public evidence constructors consistent"
    )]
    const fn new_for_channel(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        instrument: InstrumentId,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        channel: KrakenChannel,
    ) -> Self {
        Self {
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            instrument,
            freshness,
            budget,
            channel,
        }
    }

    /// Builds metadata capped below execution quality by the reviewed no-sequence decision.
    ///
    /// # Errors
    ///
    /// Rejects invalid evidence relationships or a rights/budget declaration inconsistent with
    /// the source framework.
    pub fn try_build(self) -> Result<SourceMetadata, KrakenMetadataError> {
        let version = RuleVersion::new(KRAKEN_QUALIFICATION_POLICY_VERSION)
            .map_err(|_| KrakenMetadataError::Rule)?;
        let make_rule = |name: &'static str| -> Result<IntegrityRule, KrakenMetadataError> {
            Ok(IntegrityRule::new(
                SourceIdentifier::try_from(name).map_err(|_| KrakenMetadataError::Rule)?,
                version,
            ))
        };
        let (rules, channel_id) = match self.channel {
            KrakenChannel::Book(_) => (
                vec![
                    LiveCoverageRule::try_new(
                        LiveEventClass::BookSnapshot,
                        Some(MarketDepth::PriceLevel),
                        SnapshotApplicability::Required,
                    )?,
                    LiveCoverageRule::try_new(
                        LiveEventClass::BookDelta,
                        Some(MarketDepth::PriceLevel),
                        SnapshotApplicability::Required,
                    )?,
                ],
                "book-v2",
            ),
            KrakenChannel::Trades => (
                vec![LiveCoverageRule::try_new(
                    LiveEventClass::Trade,
                    None,
                    SnapshotApplicability::NotApplicable {
                        metadata_rule: make_rule("kraken-ws-v2-trade-snapshot-na-v1")?,
                    },
                )?],
                "trade-v2",
            ),
        };
        let live = LiveCoverageDeclaration::try_new(
            ProviderProduct::new(SourceIdentifier::try_from("kraken-spot")?),
            ProviderChannel::new(SourceIdentifier::try_from(channel_id)?),
            rules,
        )?;
        let coverage = SourceCoverage::try_instrument(
            self.coverage_evidence,
            self.effective,
            vec![AssetClass::Crypto],
            CoverageTopology::single_venue(VenueId::try_from("kraken")?),
            InstrumentCoverage::enumerated(vec![self.instrument])?,
            Some(live),
            CoverageDelay::RealTime,
            DeliveryEvidence::DirectVenue,
        )?;
        let checksum = match self.channel {
            KrakenChannel::Book(_) => ChecksumValidationProfile::Provided {
                rule: make_rule("kraken-ws-v2-book-checksum-v1")?,
                algorithm: ChecksumAlgorithm::Crc32IsoHdlc,
                canonicalization: SourceIdentifier::try_from(KRAKEN_V2_CANONICALIZATION_ID)?,
                scope: SourceIdentifier::try_from(KRAKEN_V2_SCOPE_ID)?,
                book_scope: Some(ChecksumBookScope::new(
                    MarketDepth::PriceLevel,
                    NonZeroU16::new(10),
                )),
            },
            KrakenChannel::Trades => ChecksumValidationProfile::Unsupported {
                rule: make_rule("kraken-ws-v2-trade-checksum-unsupported-v1")?,
            },
        };
        let checksum_capability = match self.channel {
            KrakenChannel::Book(_) => ChecksumCapability::Provided,
            KrakenChannel::Trades => ChecksumCapability::Unsupported,
        };
        let sequence_rule = match self.channel {
            KrakenChannel::Book(_) => KRAKEN_BOOK_SEQUENCE_RULE,
            KrakenChannel::Trades => KRAKEN_TRADE_SEQUENCE_RULE,
        };
        let protocol = LiveProtocolProfile::new(
            make_rule("kraken-ws-v2-decoder-policy-v1")?,
            SemanticInterpretationProfile::new(
                make_rule("kraken-ws-v2-trade-taker-side-v1")?,
                make_rule("kraken-ws-v2-auction-unsupported-v1")?,
                make_rule("kraken-ws-v2-system-status-v1")?,
                make_rule("kraken-ws-v2-corporate-action-unsupported-v1")?,
            ),
            make_rule("kraken-ws-v2-rfc3339-timestamp-v1")?,
            SequenceValidationProfile::Unsupported {
                rule: make_rule(sequence_rule)?,
            },
            checksum,
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        );
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            self.source_id,
            self.revision_evidence,
            SourceClass::Exchange,
            SourceIdentifier::try_from("kraken")?,
            self.authorization,
            coverage,
            DataQuality::DirectUnverified,
            NetworkAccessPolicy::Allowlisted(market_squawk_sources::EndpointPolicy::try_new([
                "wss://ws.kraken.com/v2",
            ])?),
            self.freshness,
            Some(self.budget),
            SourceCapabilities::new(
                true,
                false,
                SequenceCapability::Unsupported,
                checksum_capability,
                HistoricalCapability::None,
                true,
            ),
            SourceProtocolProfile::Live(Box::new(protocol)),
        ))?)
    }
}

/// Kraken metadata construction error.
#[derive(Debug, Error)]
pub enum KrakenMetadataError {
    /// A source-framework relationship was invalid.
    #[error(transparent)]
    Metadata(#[from] SourceMetadataError),
    /// A provider/network policy was invalid.
    #[error(transparent)]
    Network(#[from] market_squawk_sources::NetworkPolicyError),
    /// A bounded domain identity was invalid.
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// A compiled provider rule identity was invalid.
    #[error("compiled Kraken rule identity is invalid")]
    Rule,
}

/// Immutable independent decision about the maximum usable quality of Kraken book data.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KrakenQualificationPolicy;

impl KrakenQualificationPolicy {
    /// Returns the reviewed policy compiled into this adapter.
    pub const fn current() -> Self {
        Self
    }

    /// Returns the reviewed policy version.
    pub const fn version(self) -> u32 {
        KRAKEN_QUALIFICATION_POLICY_VERSION
    }

    /// Returns the content digest for the reviewed policy decision.
    pub const fn digest(self) -> &'static str {
        KRAKEN_QUALIFICATION_POLICY_DIGEST
    }

    /// Returns the maximum quality allowed by the absent book sequence capability.
    pub const fn quality_ceiling(self) -> DataQuality {
        DataQuality::DirectUnverified
    }

    /// Returns immediate automated-action eligibility.
    pub const fn execution_eligibility(self) -> ExecutionEligibility {
        ExecutionEligibility::Ineligible
    }
}
