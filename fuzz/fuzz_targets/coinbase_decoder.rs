#![no_main]

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr as _;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use market_squawk_adapter_coinbase::{
    CoinbaseChannel, CoinbaseExchangeConfig, CoinbaseExchangeDecoder, CoinbaseProductMapping,
    CoinbaseTransportLimits,
};
use market_squawk_domain::{
    AuthorizationBasis, ConnectionGeneration, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, MetadataRevision, ProviderProduct,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    FreshnessPolicy, MarketDecoder, ProviderBudgetPolicy, SessionId, TransportFrameKind,
};

const MAX_INPUT_BYTES: usize = 256 * 1024;
static CONFIG: OnceLock<Option<CoinbaseExchangeConfig>> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > MAX_INPUT_BYTES {
        return;
    }
    let Some(config) = CONFIG.get_or_init(config).as_ref() else {
        return;
    };
    let Ok(mut registry) = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics() else {
        return;
    };
    let Ok(registered) =
        registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))
    else {
        return;
    };
    let Ok(session_id) = SourceIdentifier::try_from("coinbase-fuzz-session") else {
        return;
    };
    let Ok(generation) = ConnectionGeneration::new(1) else {
        return;
    };
    let Ok(session) = registry.begin_session(
        &registered,
        SessionId::new(session_id),
        generation,
        Timestamp::from_unix_nanos(1),
    ) else {
        return;
    };
    let Ok(mut frames) = registry.take_raw_frame_factory(&session) else {
        return;
    };
    let Ok(frame) = frames.try_frame(TransportFrameKind::Text, Bytes::copy_from_slice(data)) else {
        return;
    };
    let Ok(validated) = session.validate_live_frame(&frame) else {
        return;
    };
    let Ok(mut decoder) = CoinbaseExchangeDecoder::try_new(config) else {
        return;
    };
    let _outcome = decoder.decode(&validated);
});

fn config() -> Option<CoinbaseExchangeConfig> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None).ok()?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(identifier("coinbase-public-interface-v1")?),
        evidence(2),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(identifier("coinbase-exchange")?, &authorization).ok()?,
        NonZeroU32::new(8)?,
        NonZeroU64::new(1_000_000_000)?,
        NonZeroU16::new(1)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000)?,
            NonZeroU64::new(1_000_000_000)?,
            1_000,
        )
        .ok()?,
    )
    .ok()?;
    let mapping = CoinbaseProductMapping::try_new(
        ProviderProduct::new(identifier("BTC-USD")?),
        InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed").ok()?,
    )
    .ok()?;
    CoinbaseExchangeConfig::try_new(
        SourceId::try_from("coinbase-exchange-public").ok()?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("exchange-v1-2026-07-20")?),
            evidence(3),
        ),
        authorization,
        evidence(4),
        effective,
        vec![mapping],
        vec![
            CoinbaseChannel::Level2,
            CoinbaseChannel::MarketTrades,
            CoinbaseChannel::Heartbeats,
        ],
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )
        .ok()?,
        budget,
        CoinbaseTransportLimits::try_new(
            MAX_INPUT_BYTES,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .ok()?,
    )
    .ok()
}

fn identifier(value: &str) -> Option<SourceIdentifier> {
    SourceIdentifier::try_from(value).ok()
}

fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}
