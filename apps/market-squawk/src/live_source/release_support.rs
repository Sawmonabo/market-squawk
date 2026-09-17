//! Offline release evidence over the production Coinbase profile and decoder.

use std::collections::BTreeMap;
use std::ffi::OsString;

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use market_squawk_domain::{
    ConnectionGeneration, DataQuality, LiveEventClass, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{ConfigOverrides, ConfigSources};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, CurrentSourceSession,
    DecodeOutcome, MarketDecoder, NetworkAccessPolicy, RawFrameFactory, SessionId, SourceClass,
    SourceMetadata, SourceMetadataInput, TransportFrameKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::composition::ProductionCoinbaseProfile;
use crate::AppConfig;

const COINBASE_CONFIG: &str = r#"{
  "endpoint":"wss://advanced-trade-ws.coinbase.com",
  "event_classes":["book_snapshot","book_delta","trade"],
  "depth":"price_level",
  "freshness_ms":5000,
  "max_frame_bytes":16777216,
  "subscription_ack_timeout_ms":5000,
  "control_message_capacity":64,
  "control_byte_capacity":65536,
  "authorization":{
    "mode":"public_interface",
    "provider":"coinbase-exchange",
    "basis":"user-reviewed-coinbase-public-interface",
    "evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "evidence_reference":"https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview",
    "evidence_version":"reviewed-2026-08-08",
    "effective_from_unix_nanos":1700000000000000000,
    "effective_until_unix_nanos":1900000000000000000
  },
  "instruments":[{
    "product":"BTC-USD",
    "instrument_id":"4c74ab95-53b9-42ad-9b66-0ed403b88fed",
    "definition_revision":1,
    "asset_class":"crypto",
    "primary_asset":"b9f6d14f-9140-4ca3-a412-9bd59b3b5e67",
    "quote_currency":"USD",
    "tick_size":"0.01",
    "lot_size":"0.00000001",
    "contract_multiplier":"1",
    "venue":"coinbase-exchange",
    "trading_status":"active"
  }]
}"#;
const SNAPSHOT: &[u8] =
    include_bytes!("../../../../adapters/market-squawk-adapter-coinbase/fixtures/snapshot.json");
const DELTA: &[u8] =
    include_bytes!("../../../../adapters/market-squawk-adapter-coinbase/fixtures/l2update.json");
const TRADE: &[u8] =
    include_bytes!("../../../../adapters/market-squawk-adapter-coinbase/fixtures/match.json");

/// Evidence that the production Coinbase profile and decoder admitted the sealed local fixtures.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoinbaseReleaseEvidence {
    source_id: String,
    endpoint: String,
    quality_ceiling: String,
    automated_action_eligible: bool,
    snapshot_observations: usize,
    delta_observations: usize,
    trade_observations: usize,
    snapshot_sha256: [u8; 32],
    delta_sha256: [u8; 32],
    trade_sha256: [u8; 32],
}

pub(crate) fn run_coinbase_release_evidence() -> Result<CoinbaseReleaseEvidence> {
    let environment = BTreeMap::from([(
        OsString::from("MARKET_SQUAWK_COINBASE_JSON"),
        OsString::from(COINBASE_CONFIG),
    )]);
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides::default(),
    ))
    .context("production Coinbase release profile configuration is invalid")?;
    let source = config
        .coinbase()
        .context("production Coinbase release profile is absent")?;
    let now = Timestamp::from_unix_nanos(1_800_000_000_000_000_000);
    let profile = ProductionCoinbaseProfile::try_from_at(source, now)
        .context("production Coinbase release profile admission failed")?;
    if profile.metadata().quality_ceiling() != DataQuality::DirectUnverified {
        bail!("public Coinbase profile changed its execution-quality ceiling");
    }

    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()
        .context("Coinbase release frame authority initialization failed")?;
    let metadata = offline_metadata(profile.metadata())?;
    let registered = registry
        .register(metadata, now)
        .context("Coinbase release frame profile registration failed")?;
    let session = registry
        .begin_session(
            &registered,
            SessionId::new(SourceIdentifier::try_from(
                "coinbase-release-demonstration",
            )?),
            ConnectionGeneration::new(1)?,
            now,
        )
        .context("Coinbase release frame session failed")?;
    let mut frames = registry
        .take_raw_frame_factory(&session)
        .context("Coinbase release frame factory is unavailable")?;
    let mut decoder = profile.decoder().clone();
    let snapshot_observations = decode_count(
        &session,
        &mut frames,
        &mut decoder,
        SNAPSHOT,
        LiveEventClass::BookSnapshot,
    )?;
    let delta_observations = decode_count(
        &session,
        &mut frames,
        &mut decoder,
        DELTA,
        LiveEventClass::BookDelta,
    )?;
    let trade_observations = decode_count(
        &session,
        &mut frames,
        &mut decoder,
        TRADE,
        LiveEventClass::Trade,
    )?;
    registry
        .end_session(&session, now)
        .context("Coinbase release frame session close failed")?;

    Ok(CoinbaseReleaseEvidence {
        source_id: profile.metadata().source_id().as_str().to_owned(),
        endpoint: profile.endpoint().to_owned(),
        quality_ceiling: "direct_unverified".to_owned(),
        automated_action_eligible: false,
        snapshot_observations,
        delta_observations,
        trade_observations,
        snapshot_sha256: digest(SNAPSHOT),
        delta_sha256: digest(DELTA),
        trade_sha256: digest(TRADE),
    })
}

fn decode_count(
    session: &CurrentSourceSession,
    frames: &mut RawFrameFactory,
    decoder: &mut market_squawk_adapter_coinbase::CoinbaseExchangeDecoder,
    payload: &'static [u8],
    expected: LiveEventClass,
) -> Result<usize> {
    let frame = frames
        .try_frame(TransportFrameKind::Text, Bytes::from_static(payload))
        .context("Coinbase release fixture could not become a raw frame")?;
    let validated = session
        .validate_live_frame(&frame)
        .context("Coinbase release fixture frame authority failed")?;
    let DecodeOutcome::Data(batch) = decoder
        .decode(&validated)
        .context("production Coinbase decoder failed")?
    else {
        bail!("production Coinbase decoder did not produce market data");
    };
    if batch.observations().len() != 1
        || batch
            .observations()
            .first()
            .is_none_or(|observation| observation.event_class() != expected)
    {
        bail!("production Coinbase decoder returned an unexpected event class");
    }
    Ok(batch.observations().len())
}

fn offline_metadata(metadata: &SourceMetadata) -> Result<SourceMetadata> {
    SourceMetadata::try_new(SourceMetadataInput::new(
        metadata.schema_version(),
        metadata.source_id().clone(),
        metadata.revision_evidence().clone(),
        SourceClass::LocalFile,
        metadata.provider().clone(),
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            metadata.authorization().basis().clone(),
            metadata.authorization().evidence().clone(),
            metadata.authorization().effective_interval(),
        ),
        metadata.coverage().clone(),
        metadata.quality_ceiling(),
        NetworkAccessPolicy::Denied,
        metadata.freshness_policy(),
        None,
        metadata.capabilities(),
        metadata.protocol_profile().clone(),
    ))
    .context("offline Coinbase release frame metadata is invalid")
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
