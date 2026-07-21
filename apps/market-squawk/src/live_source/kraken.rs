//! Sealed platform-to-Kraken production profile.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_kraken::{
    KrakenConfig, KrakenConfigError, KrakenDepth, KrakenMarketDecoder, KrakenMetadataError,
    KrakenMetadataInput, KrakenSource,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, IdentityError, MetadataRevision,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{KrakenAuthorizationAttestation, KrakenSourceConfig};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, DecodeError,
    FreshnessPolicy, LiveSourceGeneration, ProviderBudgetPolicy, SourceError, SourceMetadata,
    SourceMetadataProvider,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

const SOURCE_ID: &str = "kraken-public-book-v2";
const IMPLEMENTATION_PROFILE_VERSION: &str = "kraken-book-v2-profile-2026-07-21";
const PROFILE_EVIDENCE_DOMAIN: &[u8] = b"market-squawk/kraken-production-profile/v1\0";
const REQUESTS_PER_WINDOW: u32 = 8;
const REQUEST_WINDOW_NANOS: u64 = 1_000_000_000;
const MAX_CONCURRENT_REQUESTS: u16 = 1;
const INITIAL_BACKOFF_NANOS: u64 = 250_000_000;
const MAXIMUM_BACKOFF_NANOS: u64 = 30_000_000_000;
const BACKOFF_JITTER_BASIS_POINTS: u16 = 2_000;
const MAX_CLOCK_SKEW_NANOS: u64 = 1_000_000_000;

/// Complete immutable Kraken provider profile derived from strict local configuration.
#[derive(Debug)]
pub(super) struct ProductionKrakenProfile {
    adapter_config: KrakenConfig,
}

impl ProductionKrakenProfile {
    pub(super) fn try_from_at(
        config: &KrakenSourceConfig,
        at: Timestamp,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let attestation = config.authorization();
        if attestation.provider().as_str() != "kraken" {
            return Err(ProductionKrakenProfileError::AuthorizationMismatch);
        }
        if !attestation.is_effective_at(at) {
            return Err(ProductionKrakenProfileError::AuthorizationNotEffective);
        }
        let evidence_input = KrakenProfileEvidence::try_from(config)?;
        let encoded = serde_json::to_vec(&evidence_input)
            .map_err(|_error| ProductionKrakenProfileError::EvidenceSerialization)?;
        let mut hasher = Sha256::new();
        hasher.update(PROFILE_EVIDENCE_DOMAIN);
        hasher.update(encoded);
        let digest: [u8; 32] = hasher.finalize().into();
        let profile_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest,
        ));
        let effective = attestation.effective_interval();
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::PublicInterface,
            attestation.basis().clone(),
            attestation.evidence().clone(),
            effective,
        );
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::for_authorization(attestation.provider().clone(), &authorization)?,
            nonzero_u32(REQUESTS_PER_WINDOW)?,
            nonzero_u64(REQUEST_WINDOW_NANOS)?,
            nonzero_u16(MAX_CONCURRENT_REQUESTS)?,
            BackoffPolicy::try_new(
                nonzero_u64(INITIAL_BACKOFF_NANOS)?,
                nonzero_u64(MAXIMUM_BACKOFF_NANOS)?,
                BACKOFF_JITTER_BASIS_POINTS,
            )?,
        )?;
        let freshness_nanos = duration_nanos(config.freshness())?;
        let freshness = FreshnessPolicy::try_new(
            freshness_nanos,
            freshness_nanos,
            freshness_nanos,
            MAX_CLOCK_SKEW_NANOS,
            freshness_nanos,
        )?;
        let revision = RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(content_addressed_revision(digest)?),
            profile_evidence.clone(),
        );
        let metadata = KrakenMetadataInput::new(
            SourceId::try_from(SOURCE_ID)?,
            revision,
            authorization,
            profile_evidence,
            effective,
            config.definition().instrument_id(),
            freshness,
            budget,
        )
        .try_build()?;
        let adapter_config = KrakenConfig::try_new(
            metadata,
            config.symbol(),
            config.definition().instrument_id(),
            KrakenDepth::Ten,
            config.max_frame_bytes(),
        )?;
        Ok(Self { adapter_config })
    }

    pub(super) fn metadata(&self) -> &SourceMetadata {
        self.adapter_config.metadata()
    }

    pub(super) fn endpoint(&self) -> &str {
        self.adapter_config.endpoint().as_str()
    }

    pub(super) fn decoder(&self) -> Result<KrakenMarketDecoder, DecodeError> {
        KrakenMarketDecoder::try_new(
            self.metadata().clone(),
            self.adapter_config.symbol(),
            self.adapter_config.instrument(),
            KrakenDepth::Ten,
        )
    }

    pub(super) fn try_source(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<KrakenSource, SourceError> {
        KrakenSource::try_new(self.adapter_config.clone(), generation)
    }

    #[cfg(all(test, debug_assertions))]
    pub(super) fn with_local_endpoint_for_test(
        self,
        endpoint: &str,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let Self { adapter_config } = self;
        Ok(Self {
            adapter_config: adapter_config.with_local_endpoint_for_test(endpoint)?,
        })
    }
}

impl TryFrom<&KrakenSourceConfig> for ProductionKrakenProfile {
    type Error = ProductionKrakenProfileError;

    fn try_from(config: &KrakenSourceConfig) -> Result<Self, Self::Error> {
        Self::try_from_at(config, system_timestamp()?)
    }
}

#[derive(Serialize)]
struct KrakenProfileEvidence<'a> {
    implementation_profile_version: &'static str,
    endpoint: &'a str,
    symbol: &'a str,
    definition: &'a market_squawk_domain::InstrumentDefinition,
    depth: usize,
    freshness_nanos: u64,
    max_frame_bytes: usize,
    subscription_ack_timeout_nanos: u64,
    control_message_capacity: usize,
    control_byte_capacity: usize,
    authorization: &'a KrakenAuthorizationAttestation,
}

impl<'a> TryFrom<&'a KrakenSourceConfig> for KrakenProfileEvidence<'a> {
    type Error = ProductionKrakenProfileError;

    fn try_from(config: &'a KrakenSourceConfig) -> Result<Self, Self::Error> {
        let controls = config.control_limits();
        Ok(Self {
            implementation_profile_version: IMPLEMENTATION_PROFILE_VERSION,
            endpoint: config.endpoint(),
            symbol: config.symbol(),
            definition: config.definition(),
            depth: config.depth(),
            freshness_nanos: duration_nanos(config.freshness())?,
            max_frame_bytes: config.max_frame_bytes().get(),
            subscription_ack_timeout_nanos: duration_nanos(config.subscription_ack_timeout())?,
            control_message_capacity: controls.message_capacity().get(),
            control_byte_capacity: controls.byte_capacity().get(),
            authorization: config.authorization(),
        })
    }
}

fn content_addressed_revision(
    digest: [u8; 32],
) -> Result<SourceIdentifier, ProductionKrakenProfileError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut revision = String::with_capacity(74);
    revision.push_str("kraken-v2-");
    for byte in digest {
        revision.push(char::from(HEX[usize::from(byte >> 4)]));
        revision.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(SourceIdentifier::try_from(revision)?)
}

fn duration_nanos(value: Duration) -> Result<u64, ProductionKrakenProfileError> {
    u64::try_from(value.as_nanos()).map_err(|_error| ProductionKrakenProfileError::DurationRange)
}

fn system_timestamp() -> Result<Timestamp, ProductionKrakenProfileError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ProductionKrakenProfileError::ClockRange)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| ProductionKrakenProfileError::ClockRange)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn nonzero_u16(value: u16) -> Result<NonZeroU16, ProductionKrakenProfileError> {
    NonZeroU16::new(value).ok_or(ProductionKrakenProfileError::InvalidStaticPolicy)
}

fn nonzero_u32(value: u32) -> Result<NonZeroU32, ProductionKrakenProfileError> {
    NonZeroU32::new(value).ok_or(ProductionKrakenProfileError::InvalidStaticPolicy)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, ProductionKrakenProfileError> {
    NonZeroU64::new(value).ok_or(ProductionKrakenProfileError::InvalidStaticPolicy)
}

/// Kraken production-profile validation failure.
#[derive(Debug, Error)]
pub enum ProductionKrakenProfileError {
    #[error("Kraken production profile identity is invalid")]
    Identity(#[from] IdentityError),
    #[error("Kraken source metadata is invalid")]
    Metadata(#[from] KrakenMetadataError),
    #[error("Kraken adapter configuration is invalid")]
    Adapter(#[from] KrakenConfigError),
    #[error("Kraken decoder configuration is invalid")]
    Decoder(#[from] DecodeError),
    #[error("Kraken production profile evidence could not be encoded")]
    EvidenceSerialization,
    #[error("Kraken production duration exceeds the supported nanosecond range")]
    DurationRange,
    #[error("Kraken authorization attestation names another provider")]
    AuthorizationMismatch,
    #[error("Kraken authorization attestation is not effective at composition time")]
    AuthorizationNotEffective,
    #[error("Kraken production static policy contains a zero bound")]
    InvalidStaticPolicy,
    #[error("Kraken production system wall clock is invalid")]
    ClockRange,
    #[error("Kraken source network/budget policy is invalid")]
    NetworkPolicy(#[from] market_squawk_sources::NetworkPolicyError),
    #[error("Kraken source policy is invalid")]
    SourcePolicy(#[from] market_squawk_sources::SourceMetadataError),
}
