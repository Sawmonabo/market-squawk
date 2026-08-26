use std::collections::TryReserveError;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::model::{
    AuctionImbalanceSide, AuctionType, DecodedIexEvent, EpochNanos, FeedKind, FeedVersion,
    IexEvent, IexVenueSemantics, LuldTier, ModelError, OperationalHaltStatus, OrderSide,
    PriceLevelSide, PriceType, PriceUnits1e4, RetailLiquidityIndicator, SecurityEventCode,
    Sha256Digest, ShortSalePriceTestDetail, SystemEventCode, TradeDate, TradingStatus,
    TransportVersion,
};
use crate::planning::{
    ColdJobPlan, IexHistCapacityCategory, IexHistDecodeAttemptEvidence,
};
use crate::receipt::{CaptureChronologyDisposition, PcapMaterializationReceipt};

const PCAP_GLOBAL_HEADER_BYTES: usize = 24;
const PCAP_RECORD_HEADER_BYTES: usize = 16;
const IEX_TP_HEADER_BYTES: usize = 40;
const ETHERNET_HEADER_BYTES: usize = 14;
const IPV4_HEADER_BYTES: usize = 20;
const UDP_HEADER_BYTES: usize = 8;
const MAX_MESSAGES_PER_SEGMENT: usize = 4_096;
const MAX_SUPPORTED_MESSAGE_BYTES: usize = 4_096;
const MAX_CALLER_CHUNK_BYTES: usize = 1024 * 1024;
const MAX_TIMESTAMP_HOURS_FROM_TRADE_DATE: i64 = 36;
const MAX_DEEP_PLUS_CHANNELS: usize = 16;
const MAX_TIMESTAMP_KEYS: u32 = 2_000_000;
const EVENT_SCHEMA_VERSION: &str = "iex-hist-native-events/v2";
const EVENT_SERIALIZATION_VERSION: &str = "iex-hist-json-sequence/v1";
const DECODER_IMPLEMENTATION_VERSION: &str = "iex-hist-rust-decoder/v3";
const SERIALIZED_EVENT_FRAME_BYTES: u64 = 16;

/// Resource limits for one streaming PCAP decode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeLimits {
    /// Largest caller chunk accepted by `push`.
    pub max_stream_chunk_bytes: usize,
    /// Largest captured Ethernet frame accepted.
    pub max_packet_bytes: u32,
    /// Maximum PCAP records admitted for the file.
    pub max_packets: u64,
    /// Maximum higher-layer messages admitted for the file.
    pub max_messages: u64,
    /// Maximum exact framed native decoded-event batch bytes admitted by the cold plan.
    pub max_decoded_event_batch_bytes: u64,
    /// Maximum distinct provider `(message type, symbol)` timestamp coordinates retained.
    pub max_timestamp_keys: u32,
    /// Maximum absolute IEX-TP send versus PCAP-capture clock skew admitted by the plan.
    pub max_send_capture_skew_nanos: u64,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_stream_chunk_bytes: 128 * 1024,
            max_packet_bytes: 65_535,
            max_packets: 200_000_000,
            max_messages: 1_000_000_000,
            max_decoded_event_batch_bytes: 32 * 1024 * 1024 * 1024,
            max_timestamp_keys: 1_000_000,
            max_send_capture_skew_nanos: 60_000_000_000,
        }
    }
}

impl DecodeLimits {
    fn validate(self) -> Result<(), DecodeError> {
        if self.max_stream_chunk_bytes == 0
            || self.max_stream_chunk_bytes > MAX_CALLER_CHUNK_BYTES
            || self.max_packet_bytes < 64
            || self.max_packet_bytes > 65_535
            || self.max_packets == 0
            || self.max_messages == 0
            || self.max_decoded_event_batch_bytes == 0
            || self.max_timestamp_keys == 0
            || self.max_timestamp_keys > MAX_TIMESTAMP_KEYS
            || self.max_send_capture_skew_nanos == 0
        {
            Err(DecodeError::InvalidLimits)
        } else {
            Ok(())
        }
    }

    /// Returns the immutable identity of every decoder resource and clock ceiling.
    #[must_use]
    pub fn identity(self) -> Sha256Digest {
        crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-decode-limits/v1",
            &u64::try_from(self.max_stream_chunk_bytes)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
            &self.max_packet_bytes.to_le_bytes(),
            &self.max_packets.to_le_bytes(),
            &self.max_messages.to_le_bytes(),
            &self.max_decoded_event_batch_bytes.to_le_bytes(),
            &self.max_timestamp_keys.to_le_bytes(),
            &self.max_send_capture_skew_nanos.to_le_bytes(),
        ])
    }
}

/// Exact behavior required from one DEEP+ channel in a date-effective DPLC distribution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodeChannelRole {
    /// The channel must carry a complete start-to-end non-heartbeat feed session.
    Active,
    /// The exact distribution evidence proves this channel reserved and heartbeat-only.
    ReservedHeartbeatOnly,
}

/// Exact date-effective 16-channel distribution required to interpret a DPLC capture.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DplcChannelDistributionContract {
    trade_date_year: u16,
    trade_date_month: u8,
    trade_date_day: u8,
    roles: [DecodeChannelRole; MAX_DEEP_PLUS_CHANNELS],
    provider_evidence_sha256: Sha256Digest,
    contract_sha256: Sha256Digest,
}

impl DplcChannelDistributionContract {
    /// Constructs a closed 16-channel contract from exact date-effective provider evidence.
    ///
    /// # Errors
    ///
    /// Rejects zero evidence or a distribution with no active channel.
    pub(crate) fn try_new(
        trade_date: TradeDate,
        roles: [DecodeChannelRole; MAX_DEEP_PLUS_CHANNELS],
        provider_evidence_sha256: Sha256Digest,
    ) -> Result<Self, DecodeError> {
        if !nonzero_digest(provider_evidence_sha256)
            || !roles.contains(&DecodeChannelRole::Active)
        {
            return Err(DecodeError::InvalidDecoderContract);
        }
        let contract_sha256 = dplc_distribution_identity(
            trade_date,
            roles,
            provider_evidence_sha256,
        );
        Ok(Self {
            trade_date_year: trade_date.year(),
            trade_date_month: trade_date.month(),
            trade_date_day: trade_date.day(),
            roles,
            provider_evidence_sha256,
            contract_sha256,
        })
    }

    fn trade_date(self) -> Result<TradeDate, DecodeError> {
        TradeDate::new(
            self.trade_date_year,
            self.trade_date_month,
            self.trade_date_day,
        )
        .map_err(|_| DecodeError::InvalidDecoderContract)
    }

    fn validate_for(self, trade_date: TradeDate) -> Result<(), DecodeError> {
        if self.trade_date()? != trade_date
            || !nonzero_digest(self.provider_evidence_sha256)
            || !self.roles.contains(&DecodeChannelRole::Active)
            || dplc_distribution_identity(
                trade_date,
                self.roles,
                self.provider_evidence_sha256,
            ) != self.contract_sha256
        {
            return Err(DecodeError::InvalidDecoderContract);
        }
        Ok(())
    }

    /// Returns the exact role of channel `1..=16`.
    #[must_use]
    pub fn role(self, channel_id: u32) -> Option<DecodeChannelRole> {
        channel_id
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.roles.get(index).copied())
    }

    /// Returns the exact provider evidence identity proving the date-effective assignments.
    #[must_use]
    pub const fn provider_evidence_sha256(self) -> Sha256Digest {
        self.provider_evidence_sha256
    }

    /// Returns the exact distribution-contract identity.
    #[must_use]
    pub const fn contract_sha256(self) -> Sha256Digest {
        self.contract_sha256
    }
}

/// Exact channel topology selected by one immutable decoder contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "topology", content = "distribution")]
pub enum DecodeChannelContract {
    /// TOPS, DEEP, and DPLS use one active channel with identifier 1.
    SingleActiveChannelOne,
    /// DPLC requires all 16 channels under exact date-effective role evidence.
    Dplc16(DplcChannelDistributionContract),
}

/// Complete immutable decoder contract bound into planning, events, summaries, and sink commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeContract {
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    limits: DecodeLimits,
    anomaly_policy: DecodeAnomalyPolicy,
    channel_contract: DecodeChannelContract,
    implementation_sha256: Sha256Digest,
    contract_sha256: Sha256Digest,
}

impl<'de> Deserialize<'de> for DecodeContract {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireContract {
            feed: FeedKind,
            feed_version: String,
            transport_version: TransportVersion,
            limits: DecodeLimits,
            anomaly_policy: DecodeAnomalyPolicy,
            channel_contract: DecodeChannelContract,
            implementation_sha256: Sha256Digest,
            contract_sha256: Sha256Digest,
        }

        let wire = WireContract::deserialize(deserializer)?;
        let feed_version = match (wire.feed, wire.feed_version.as_str()) {
            (FeedKind::Tops, "1.6") => FeedVersion::Tops1_6,
            (FeedKind::Deep, "1.0") => FeedVersion::Deep1_0,
            (FeedKind::DeepPlusDpls, "1.0") => FeedVersion::DeepPlusDpls1_0,
            (FeedKind::DeepPlusDplc, "1") => FeedVersion::DeepPlusDplc1,
            _ => {
                return Err(serde::de::Error::custom(
                    "IEX HIST decode-contract feed/version pair is invalid",
                ));
            }
        };
        Ok(Self {
            feed: wire.feed,
            feed_version,
            transport_version: wire.transport_version,
            limits: wire.limits,
            anomaly_policy: wire.anomaly_policy,
            channel_contract: wire.channel_contract,
            implementation_sha256: wire.implementation_sha256,
            contract_sha256: wire.contract_sha256,
        })
    }
}

impl DecodeContract {
    /// Selects and fingerprints the complete decoder implementation and resource contract.
    ///
    /// # Errors
    ///
    /// Rejects mismatched feed/version/transport, invalid limits, or missing/unexpected DPLC
    /// distribution evidence.
    pub fn for_selection(
        feed_version: FeedVersion,
        transport_version: TransportVersion,
        limits: DecodeLimits,
        dplc_distribution: Option<DplcChannelDistributionContract>,
    ) -> Result<Self, DecodeError> {
        limits.validate()?;
        if transport_version != TransportVersion::IexTp1 {
            return Err(DecodeError::InvalidDecoderContract);
        }
        let feed = feed_version.feed();
        let channel_contract = match (feed, dplc_distribution) {
            (FeedKind::DeepPlusDplc, Some(distribution)) => {
                DecodeChannelContract::Dplc16(distribution)
            }
            (
                FeedKind::Tops | FeedKind::Deep | FeedKind::DeepPlusDpls,
                None,
            ) => DecodeChannelContract::SingleActiveChannelOne,
            _ => return Err(DecodeError::InvalidDecoderContract),
        };
        let anomaly_policy =
            DecodeAnomalyPolicy::RejectStructuralClockAndFamilyTimestampAnomaliesRetainExtensionsV2;
        let implementation_sha256 = decoder_implementation_fingerprint();
        let contract_sha256 = decode_contract_identity(
            feed,
            feed_version,
            transport_version,
            limits,
            anomaly_policy,
            channel_contract,
            implementation_sha256,
        );
        Ok(Self {
            feed,
            feed_version,
            transport_version,
            limits,
            anomaly_policy,
            channel_contract,
            implementation_sha256,
            contract_sha256,
        })
    }

    /// Revalidates every serialized coordinate against the selected trade date.
    pub fn validate_for(self, trade_date: TradeDate) -> Result<(), DecodeError> {
        self.limits.validate()?;
        if self.feed != self.feed_version.feed()
            || self.transport_version != TransportVersion::IexTp1
            || self.implementation_sha256 != decoder_implementation_fingerprint()
        {
            return Err(DecodeError::InvalidDecoderContract);
        }
        match (self.feed, self.channel_contract) {
            (FeedKind::DeepPlusDplc, DecodeChannelContract::Dplc16(distribution)) => {
                distribution.validate_for(trade_date)?;
            }
            (
                FeedKind::Tops | FeedKind::Deep | FeedKind::DeepPlusDpls,
                DecodeChannelContract::SingleActiveChannelOne,
            ) => {}
            _ => return Err(DecodeError::InvalidDecoderContract),
        }
        let expected = decode_contract_identity(
            self.feed,
            self.feed_version,
            self.transport_version,
            self.limits,
            self.anomaly_policy,
            self.channel_contract,
            self.implementation_sha256,
        );
        if expected != self.contract_sha256 {
            return Err(DecodeError::InvalidDecoderContract);
        }
        Ok(())
    }

    #[must_use]
    pub const fn feed(self) -> FeedKind {
        self.feed
    }

    #[must_use]
    pub const fn feed_version(self) -> FeedVersion {
        self.feed_version
    }

    #[must_use]
    pub const fn transport_version(self) -> TransportVersion {
        self.transport_version
    }

    #[must_use]
    pub const fn limits(self) -> DecodeLimits {
        self.limits
    }

    #[must_use]
    pub const fn anomaly_policy(self) -> DecodeAnomalyPolicy {
        self.anomaly_policy
    }

    #[must_use]
    pub const fn channel_contract(self) -> DecodeChannelContract {
        self.channel_contract
    }

    #[must_use]
    pub const fn implementation_sha256(self) -> Sha256Digest {
        self.implementation_sha256
    }

    #[must_use]
    pub const fn contract_sha256(self) -> Sha256Digest {
        self.contract_sha256
    }

    #[must_use]
    pub const fn feed_specification_version(self) -> &'static str {
        self.feed_version.specification_value()
    }

    #[must_use]
    pub const fn transport_specification_version(self) -> &'static str {
        self.transport_version.specification_value()
    }

    #[must_use]
    pub const fn native_schema_version(self) -> &'static str {
        EVENT_SCHEMA_VERSION
    }

    #[must_use]
    pub const fn serialization_version(self) -> &'static str {
        EVENT_SERIALIZATION_VERSION
    }

    #[must_use]
    pub const fn implementation_version(self) -> &'static str {
        DECODER_IMPLEMENTATION_VERSION
    }
}

/// Exact decoder byte actuals available on success and every post-construction failure.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeActuals {
    /// Exact PCAP bytes accepted from caller chunks and rehashed by the decoder.
    pub pcap_bytes_read: u64,
    /// Events whose exact serialization was successfully staged before terminal disposition.
    pub staged_events: u64,
    /// Exact JSON event bytes successfully staged, excluding deterministic framing.
    pub serialized_event_bytes_staged: u64,
    /// Exact admitted batch bytes: ordinal + byte length + serialized event for every staged event.
    pub decoded_event_batch_bytes_staged: u64,
}

impl DecodeActuals {
    #[must_use]
    pub const fn pcap_bytes_read(self) -> u64 {
        self.pcap_bytes_read
    }

    #[must_use]
    pub const fn staged_events(self) -> u64 {
        self.staged_events
    }

    #[must_use]
    pub const fn serialized_event_bytes_staged(self) -> u64 {
        self.serialized_event_bytes_staged
    }

    #[must_use]
    pub const fn decoded_event_batch_bytes_staged(self) -> u64 {
        self.decoded_event_batch_bytes_staged
    }
}

/// Structured failure retaining exact capacity actuals for typed settlement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeFailure {
    /// Exhaustive underlying decode classification.
    pub error: DecodeError,
    /// Exact bytes/events admitted before the failure and transactional abort.
    pub actuals: DecodeActuals,
}

impl DecodeFailure {
    #[must_use]
    pub const fn error(&self) -> &DecodeError {
        &self.error
    }

    #[must_use]
    pub const fn actuals(&self) -> DecodeActuals {
        self.actuals
    }
}

/// Terminal accounting for one completely validated PCAP.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeSummary {
    /// Parent capture receipt.
    pub capture_receipt_sha256: Sha256Digest,
    /// Exact selected provider trade date.
    pub trade_date: String,
    /// Complete immutable plan-selected decoder contract.
    pub decode_contract: DecodeContract,
    /// Exact decoder contract identity.
    pub decoder_contract_sha256: Sha256Digest,
    /// Exact fingerprint of the code-owned decoder implementation table.
    pub decoder_implementation_sha256: Sha256Digest,
    /// Exact identity of all decode limits.
    pub decode_limits_sha256: Sha256Digest,
    /// Exact authority-owned decode-attempt evidence identity.
    pub decode_attempt_evidence_sha256: Sha256Digest,
    /// Complete typed authority-owned decode-attempt evidence.
    pub decode_attempt_evidence: IexHistDecodeAttemptEvidence,
    /// Exact execution attempt identity.
    pub decode_attempt_sha256: Sha256Digest,
    /// Exact capacity request identity.
    pub decode_request_sha256: Sha256Digest,
    /// Exact durable reservation identity.
    pub decode_reservation_sha256: Sha256Digest,
    /// Shared authority generation that admitted the attempt.
    pub decode_authority_generation: u64,
    /// Exact durable storage-root identity owning the reservation.
    pub decode_storage_root_sha256: Sha256Digest,
    /// Trusted attempt-admission wall clock.
    pub decode_admitted_at_unix_nanos: i64,
    /// Trusted attempt-admission UTC offset.
    pub decode_admitted_utc_offset_seconds: i32,
    /// Trusted attempt-admission local date.
    pub decode_admitted_observed_date: String,
    /// Exact attempt deadline.
    pub decode_deadline_unix_nanos: i64,
    /// Exact PCAP bytes re-read by the decoder.
    pub pcap_bytes: u64,
    /// Validated PCAP record count.
    pub packets: u64,
    /// Validated non-heartbeat IEX-TP segment count.
    pub segments: u64,
    /// Exact higher-layer message count.
    pub messages: u64,
    /// Messages preserved as digest-only evidence rather than typed events.
    pub unmapped_messages: u64,
    /// Number of exact IEX-TP channels in the validated file.
    pub channels: u8,
    /// Ordered terminal coordinates for every observed IEX-TP channel.
    pub channel_sessions: Vec<DecodeChannelSummary>,
    /// Identity of every ordered channel/session terminal coordinate.
    pub channel_sessions_sha256: Sha256Digest,
    /// Exact first-party native feed specification selected for the catalog version family.
    pub feed_specification_version: String,
    /// Exact first-party IEX-TP specification selected for the catalog transport family.
    pub transport_specification_version: String,
    /// Code-owned native decoded-event schema version.
    pub native_schema_version: String,
    /// Code-owned ordered event serialization version.
    pub serialization_version: String,
    /// Code-owned decoder implementation version.
    pub decoder_implementation_version: String,
    /// Exact byte length of the staged ordered serialization.
    pub serialized_event_bytes: u64,
    /// Exact framed native batch bytes used for capacity settlement.
    pub decoded_event_batch_bytes: u64,
    /// Digest of every event ordinal, byte length, and exact serialized bytes in decode order.
    pub serialized_events_sha256: Sha256Digest,
    /// First PCAP capture clock observed in file order.
    pub first_capture_time_unix_nanos: u64,
    /// Last PCAP capture clock observed in file order.
    pub last_capture_time_unix_nanos: u64,
    /// First IEX-TP send clock observed in file order.
    pub first_send_time: EpochNanos,
    /// Last IEX-TP send clock observed in file order.
    pub last_send_time: EpochNanos,
    /// First typed provider source clock observed in message order.
    pub first_source_time: EpochNanos,
    /// Last typed provider source clock observed in message order.
    pub last_source_time: EpochNanos,
    /// Typed events contributing provider source clocks.
    pub source_clock_messages: u64,
    /// Distinct bounded `(message type, symbol)` timestamp coordinates retained.
    pub provider_timestamp_keys: u32,
    /// Maximum absolute send-versus-capture skew actually observed.
    pub max_observed_send_capture_skew_nanos: u64,
    /// Code-owned fail-closed clock/continuity and extension-retention policy.
    pub anomaly_policy: DecodeAnomalyPolicy,
    /// Deterministic receipt the transactional sink must return on terminal commit.
    pub sink_commit_sha256: Sha256Digest,
    /// Content identity committing all terminal decode and continuity evidence.
    pub summary_sha256: Sha256Digest,
}

/// Terminal continuity receipt for one exact IEX-TP channel/session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecodeChannelSummary {
    /// Exact IEX-TP channel identifier.
    pub channel_id: u32,
    /// Exact session identifier.
    pub session_id: u32,
    /// Next sequence expected after terminal decode.
    pub next_sequence: i64,
    /// Next stream offset expected after terminal decode.
    pub next_stream_offset: i64,
    /// Last send clock observed on this channel.
    pub last_send_time: EpochNanos,
    /// True only for an officially permitted reserved channel that carried heartbeats alone.
    pub heartbeat_only: bool,
    /// Exact plan-bound role controlling active versus reserved heartbeat-only semantics.
    pub role: DecodeChannelRole,
}

/// Code-owned anomaly disposition for the selected native decoder.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DecodeAnomalyPolicy {
    /// Reject structural, continuity, bounded skew, and per-family/symbol timestamp anomalies;
    /// retain unknown types and appended fields by exact bytes/digest.
    RejectStructuralClockAndFamilyTimestampAnomaliesRetainExtensionsV2,
}

impl DecodeSummary {
    /// Returns the terminal decode receipt identity.
    #[must_use]
    pub const fn summary_sha256(&self) -> Sha256Digest {
        self.summary_sha256
    }

    /// Returns the exact successful decode actuals for transactional capacity settlement.
    #[must_use]
    pub const fn actuals(&self) -> DecodeActuals {
        DecodeActuals {
            pcap_bytes_read: self.pcap_bytes,
            staged_events: self.messages,
            serialized_event_bytes_staged: self.serialized_event_bytes,
            decoded_event_batch_bytes_staged: self.decoded_event_batch_bytes,
        }
    }

    pub(crate) fn validate_against(
        &self,
        plan: &ColdJobPlan,
        receipt: &PcapMaterializationReceipt,
        attempt: IexHistDecodeAttemptEvidence,
    ) -> Result<(), DecodeError> {
        let trade_date = TradeDate::parse(&self.trade_date)
            .map_err(|_| DecodeError::SummaryIdentityMismatch)?;
        self.decode_contract.validate_for(trade_date)?;
        attempt
            .validate_against(plan)
            .map_err(|_| DecodeError::SummaryIdentityMismatch)?;
        receipt
            .validate_against(plan)
            .map_err(|_| DecodeError::SummaryIdentityMismatch)?;
        let admitted_clock = attempt.admitted_clock();
        let expected_batch_bytes = self
            .messages
            .checked_mul(SERIALIZED_EVENT_FRAME_BYTES)
            .and_then(|framing| framing.checked_add(self.serialized_event_bytes))
            .ok_or(DecodeError::SummaryIdentityMismatch)?;
        if trade_date != plan.selected_file.trade_date
            || self.decode_contract != plan.decode_contract()
            || self.decoder_contract_sha256 != self.decode_contract.contract_sha256()
            || self.decoder_implementation_sha256
                != self.decode_contract.implementation_sha256()
            || self.decode_limits_sha256 != self.decode_contract.limits().identity()
            || self.decode_attempt_evidence != attempt
            || self.decode_attempt_evidence_sha256 != attempt.evidence_sha256()
            || self.decode_attempt_sha256 != attempt.attempt_sha256()
            || self.decode_request_sha256 != attempt.request_sha256()
            || self.decode_reservation_sha256 != attempt.reservation_sha256()
            || self.decode_authority_generation != attempt.authority_generation()
            || self.decode_storage_root_sha256 != attempt.storage_root_sha256()
            || self.decode_admitted_at_unix_nanos != admitted_clock.unix_nanos()
            || self.decode_admitted_utc_offset_seconds != admitted_clock.utc_offset_seconds()
            || self.decode_admitted_observed_date != admitted_clock.observed_date().compact()
            || self.decode_deadline_unix_nanos != attempt.deadline_unix_nanos()
            || self.capture_receipt_sha256 != receipt.receipt_sha256
            || self.pcap_bytes != receipt.pcap_bytes
            || receipt.chronology_disposition() != CaptureChronologyDisposition::Admitted
            || self.packets == 0
            || self.segments == 0
            || self.segments > self.packets
            || self.messages == 0
            || self.unmapped_messages > self.messages
            || self.channels == 0
            || usize::from(self.channels) > MAX_DEEP_PLUS_CHANNELS
            || usize::from(self.channels) != self.channel_sessions.len()
            || self.feed_specification_version
                != self.decode_contract.feed_specification_version()
            || self.transport_specification_version
                != self.decode_contract.transport_specification_version()
            || self.native_schema_version != self.decode_contract.native_schema_version()
            || self.serialization_version != self.decode_contract.serialization_version()
            || self.decoder_implementation_version
                != self.decode_contract.implementation_version()
            || self.serialized_event_bytes == 0
            || self.decoded_event_batch_bytes != expected_batch_bytes
            || self.decoded_event_batch_bytes
                > self.decode_contract.limits().max_decoded_event_batch_bytes
            || self.first_capture_time_unix_nanos > self.last_capture_time_unix_nanos
            || self.source_clock_messages == 0
            || self.source_clock_messages > self.messages
            || self.provider_timestamp_keys == 0
            || self.provider_timestamp_keys > self.decode_contract.limits().max_timestamp_keys
            || self.max_observed_send_capture_skew_nanos
                > self.decode_contract.limits().max_send_capture_skew_nanos
            || self.anomaly_policy != self.decode_contract.anomaly_policy()
        {
            return Err(DecodeError::SummaryIdentityMismatch);
        }
        let expected_channel_identity = validate_channel_summaries(
            self.decode_contract,
            trade_date,
            &self.channel_sessions,
        )?;
        if expected_channel_identity != self.channel_sessions_sha256 {
            return Err(DecodeError::SummaryIdentityMismatch);
        }
        if decode_summary_identity(self) != self.summary_sha256 {
            return Err(DecodeError::SummaryIdentityMismatch);
        }
        if sink_commit_identity(self.summary_sha256) != self.sink_commit_sha256 {
            return Err(DecodeError::SummaryIdentityMismatch);
        }
        Ok(())
    }
}

/// Transactional event staging boundary used by the streaming decoder.
pub trait IexEventSink {
    /// Sink-owned failure type; decoder publication always maps it to a closed decode failure.
    type Error;

    /// Stages one exact serialized event under its zero-based decode ordinal. The native batch
    /// representation is the ordinal as little-endian `u64`, serialized length as little-endian
    /// `u64`, then these exact JSON bytes.
    ///
    /// # Errors
    ///
    /// Staging is atomic for this event: success retains the complete framed event and failure
    /// retains none of it. Returning an error aborts the complete transaction. Staged bytes must
    /// remain invisible to readers until `commit` accepts the terminal decode receipt.
    fn stage(&mut self, ordinal: u64, serialized_event: &[u8]) -> Result<(), Self::Error>;

    /// Atomically publishes every staged event and returns the exact expected commit identity.
    fn commit(&mut self, summary: &DecodeSummary) -> Result<Sha256Digest, Self::Error>;

    /// Discards every staged event. Implementations must make this idempotent.
    fn abort(&mut self);
}

#[derive(Clone, Copy)]
struct DecodeAttemptCoordinates {
    evidence_sha256: Sha256Digest,
    attempt_sha256: Sha256Digest,
    request_sha256: Sha256Digest,
    reservation_sha256: Sha256Digest,
    authority_generation: u64,
    storage_root_sha256: Sha256Digest,
    admitted_at_unix_nanos: i64,
    admitted_utc_offset_seconds: i32,
    admitted_observed_date: TradeDate,
    deadline_unix_nanos: i64,
}

/// Incremental classic-PCAP and IEX-TP decoder bound to one exact selected file and receipt.
pub struct PcapStreamDecoder<S: IexEventSink> {
    trade_date: TradeDate,
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    source_file_identity: Sha256Digest,
    expected_pcap_sha256: Sha256Digest,
    expected_pcap_bytes: u64,
    capture_receipt_sha256: Sha256Digest,
    contract: DecodeContract,
    attempt_evidence: IexHistDecodeAttemptEvidence,
    attempt: DecodeAttemptCoordinates,
    limits: DecodeLimits,
    pcap_hasher: Sha256,
    bytes_seen: u64,
    buffer: Vec<u8>,
    pcap_format: Option<PcapFormat>,
    channels: [Option<ChannelState>; MAX_DEEP_PLUS_CHANNELS],
    previous_capture_time: Option<u64>,
    first_capture_time: Option<u64>,
    first_send_time: Option<EpochNanos>,
    last_send_time: Option<EpochNanos>,
    first_source_time: Option<EpochNanos>,
    last_source_time: Option<EpochNanos>,
    source_clock_messages: u64,
    provider_timestamps: Vec<ProviderTimestampEntry>,
    max_observed_send_capture_skew_nanos: u64,
    serialized_event_bytes: u64,
    decoded_event_batch_bytes: u64,
    staged_events: u64,
    serialized_events_hasher: Sha256,
    packets: u64,
    segments: u64,
    messages: u64,
    unmapped_messages: u64,
    sink: Option<S>,
    poisoned: bool,
}

impl<S: IexEventSink> std::fmt::Debug for PcapStreamDecoder<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PcapStreamDecoder")
            .field("trade_date", &self.trade_date)
            .field("feed", &self.feed)
            .field("feed_version", &self.feed_version)
            .field("transport_version", &self.transport_version)
            .field("decoder_contract_sha256", &self.contract.contract_sha256())
            .field("bytes_seen", &self.bytes_seen)
            .field("packets", &self.packets)
            .field("messages", &self.messages)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl<S: IexEventSink> PcapStreamDecoder<S> {
    /// Creates a decoder only when plan, selected descriptor, gzip receipt, and PCAP bounds agree.
    ///
    /// # Errors
    ///
    /// Rejects unsupported feed versions and any parent/digest/size mismatch before bytes decode.
    pub fn new(
        plan: &ColdJobPlan,
        receipt: &PcapMaterializationReceipt,
        attempt: IexHistDecodeAttemptEvidence,
        sink: S,
    ) -> Result<Self, DecodeFailure> {
        let mut sink = Some(sink);
        let result = (|| -> Result<Self, DecodeError> {
        let contract = plan.decode_contract();
        contract.validate_for(plan.selected_file.trade_date)?;
        let limits = contract.limits();
        if contract.feed() != plan.selected_file.feed
            || contract.feed_version() != plan.selected_file.feed_version
            || contract.transport_version() != plan.selected_file.transport_version
            || plan.capacity_footprint().bytes(IexHistCapacityCategory::DecodedEventBatch)
                != limits.max_decoded_event_batch_bytes
        {
            return Err(DecodeError::InvalidDecoderContract);
        }
        attempt
            .validate_against(plan)
            .map_err(|_| DecodeError::InvalidDecodeAttempt)?;
        if attempt.decode_contract_sha256() != contract.contract_sha256() {
            return Err(DecodeError::InvalidDecodeAttempt);
        }
        receipt
            .validate_against(plan)
            .map_err(|_| DecodeError::ReceiptMismatch)?;
        if receipt.chronology_disposition() != CaptureChronologyDisposition::Admitted {
            return Err(DecodeError::CaptureChronologyQuarantined);
        }
        let initial_capacity = limits
            .max_stream_chunk_bytes
            .min(usize::try_from(limits.max_packet_bytes).unwrap_or(usize::MAX))
            .max(PCAP_GLOBAL_HEADER_BYTES);
        let mut buffer = Vec::new();
        buffer
            .try_reserve(initial_capacity)
            .map_err(|_| DecodeError::Capacity)?;
        let mut provider_timestamps = Vec::new();
        provider_timestamps
            .try_reserve_exact(
                usize::try_from(limits.max_timestamp_keys)
                    .map_err(|_| DecodeError::InvalidLimits)?,
            )
            .map_err(|_| DecodeError::Capacity)?;
        let admitted_clock = attempt.admitted_clock();
        Ok(Self {
            trade_date: plan.selected_file.trade_date,
            feed: plan.selected_file.feed,
            feed_version: plan.selected_file.feed_version,
            transport_version: plan.selected_file.transport_version,
            source_file_identity: receipt.receipt_sha256,
            expected_pcap_sha256: receipt.pcap_sha256,
            expected_pcap_bytes: receipt.pcap_bytes,
            capture_receipt_sha256: receipt.receipt_sha256,
            contract,
            attempt_evidence: attempt,
            attempt: DecodeAttemptCoordinates {
                evidence_sha256: attempt.evidence_sha256(),
                attempt_sha256: attempt.attempt_sha256(),
                request_sha256: attempt.request_sha256(),
                reservation_sha256: attempt.reservation_sha256(),
                authority_generation: attempt.authority_generation(),
                storage_root_sha256: attempt.storage_root_sha256(),
                admitted_at_unix_nanos: admitted_clock.unix_nanos(),
                admitted_utc_offset_seconds: admitted_clock.utc_offset_seconds(),
                admitted_observed_date: admitted_clock.observed_date(),
                deadline_unix_nanos: attempt.deadline_unix_nanos(),
            },
            limits,
            pcap_hasher: Sha256::new(),
            bytes_seen: 0,
            buffer,
            pcap_format: None,
            channels: [None; MAX_DEEP_PLUS_CHANNELS],
            previous_capture_time: None,
            first_capture_time: None,
            first_send_time: None,
            last_send_time: None,
            first_source_time: None,
            last_source_time: None,
            source_clock_messages: 0,
            provider_timestamps,
            max_observed_send_capture_skew_nanos: 0,
            serialized_event_bytes: 0,
            decoded_event_batch_bytes: 0,
            staged_events: 0,
            serialized_events_hasher: Sha256::new(),
            packets: 0,
            segments: 0,
            messages: 0,
            unmapped_messages: 0,
            sink: Some(sink.take().ok_or(DecodeError::SinkRejected)?),
            poisoned: false,
        })
        })();
        match result {
            Ok(decoder) => Ok(decoder),
            Err(error) => {
                if let Some(sink) = sink.as_mut() {
                    sink.abort();
                }
                Err(DecodeFailure {
                    error,
                    actuals: DecodeActuals::default(),
                })
            }
        }
    }

    /// Pushes one bounded PCAP byte chunk and emits only completely validated messages.
    ///
    /// # Errors
    ///
    /// Any framing, checksum, continuity, message, resource, or sink failure poisons the session.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), DecodeFailure> {
        if self.poisoned {
            return Err(self.failure(DecodeError::Poisoned));
        }
        let result = self.push_inner(bytes);
        if let Err(error) = result {
            self.poisoned = true;
            self.abort_sink();
            return Err(self.failure(error));
        }
        Ok(())
    }

    /// Finalizes only a complete, checksum-identical, start-to-end feed session.
    ///
    /// # Errors
    ///
    /// Rejects prior failure, truncation, checksum mismatch, empty captures, or a missing session
    /// start/end marker.
    pub fn finish(mut self) -> Result<(DecodeSummary, S), DecodeFailure> {
        let result = (|| -> Result<(DecodeSummary, S), DecodeError> {
            if self.poisoned {
                return Err(DecodeError::Poisoned);
            }
            if !self.buffer.is_empty() || self.pcap_format.is_none() {
                return Err(DecodeError::TruncatedPcap);
            }
            if self.bytes_seen != self.expected_pcap_bytes {
                return Err(DecodeError::PcapLengthMismatch);
            }
            let actual = Sha256Digest::from_bytes(self.pcap_hasher.clone().finalize().into());
            if actual != self.expected_pcap_sha256 {
                return Err(DecodeError::PcapChecksumMismatch);
            }
            if self.packets == 0
                || self.segments == 0
                || self.messages == 0
                || self.staged_events != self.messages
                || self.decoded_event_batch_bytes == 0
                || self.decoded_event_batch_bytes > self.limits.max_decoded_event_batch_bytes
            {
                return Err(DecodeError::IncompleteSession);
            }
            let (channel_sessions, channel_sessions_sha256) = terminal_channel_summaries(
                self.contract,
                self.trade_date,
                &self.channels,
            )?;
            let channels = u8::try_from(channel_sessions.len())
                .map_err(|_| DecodeError::IncompleteSession)?;
            let provider_timestamp_keys = u32::try_from(self.provider_timestamps.len())
                .map_err(|_| DecodeError::ProviderTimestampStateLimit)?;
            let serialized_events_sha256 =
                Sha256Digest::from_bytes(self.serialized_events_hasher.clone().finalize().into());
            let first_capture_time_unix_nanos = self
                .first_capture_time
                .ok_or(DecodeError::IncompleteSession)?;
            let last_capture_time_unix_nanos = self
                .previous_capture_time
                .ok_or(DecodeError::IncompleteSession)?;
            let first_send_time = self.first_send_time.ok_or(DecodeError::IncompleteSession)?;
            let last_send_time = self.last_send_time.ok_or(DecodeError::IncompleteSession)?;
            let first_source_time = self
                .first_source_time
                .ok_or(DecodeError::IncompleteSession)?;
            let last_source_time = self.last_source_time.ok_or(DecodeError::IncompleteSession)?;
            let mut summary = DecodeSummary {
                capture_receipt_sha256: self.capture_receipt_sha256,
                trade_date: self.trade_date.compact(),
                decode_contract: self.contract,
                decoder_contract_sha256: self.contract.contract_sha256(),
                decoder_implementation_sha256: self.contract.implementation_sha256(),
                decode_limits_sha256: self.limits.identity(),
                decode_attempt_evidence_sha256: self.attempt.evidence_sha256,
                decode_attempt_evidence: self.attempt_evidence,
                decode_attempt_sha256: self.attempt.attempt_sha256,
                decode_request_sha256: self.attempt.request_sha256,
                decode_reservation_sha256: self.attempt.reservation_sha256,
                decode_authority_generation: self.attempt.authority_generation,
                decode_storage_root_sha256: self.attempt.storage_root_sha256,
                decode_admitted_at_unix_nanos: self.attempt.admitted_at_unix_nanos,
                decode_admitted_utc_offset_seconds: self.attempt.admitted_utc_offset_seconds,
                decode_admitted_observed_date: self.attempt.admitted_observed_date.compact(),
                decode_deadline_unix_nanos: self.attempt.deadline_unix_nanos,
                pcap_bytes: self.bytes_seen,
                packets: self.packets,
                segments: self.segments,
                messages: self.messages,
                unmapped_messages: self.unmapped_messages,
                channels,
                channel_sessions,
                channel_sessions_sha256,
                feed_specification_version: self.contract.feed_specification_version().to_owned(),
                transport_specification_version: self
                    .contract
                    .transport_specification_version()
                    .to_owned(),
                native_schema_version: self.contract.native_schema_version().to_owned(),
                serialization_version: self.contract.serialization_version().to_owned(),
                decoder_implementation_version: self.contract.implementation_version().to_owned(),
                serialized_event_bytes: self.serialized_event_bytes,
                decoded_event_batch_bytes: self.decoded_event_batch_bytes,
                serialized_events_sha256,
                first_capture_time_unix_nanos,
                last_capture_time_unix_nanos,
                first_send_time,
                last_send_time,
                first_source_time,
                last_source_time,
                source_clock_messages: self.source_clock_messages,
                provider_timestamp_keys,
                max_observed_send_capture_skew_nanos: self
                    .max_observed_send_capture_skew_nanos,
                anomaly_policy: self.contract.anomaly_policy(),
                sink_commit_sha256: Sha256Digest::of(b"uncommitted"),
                summary_sha256: Sha256Digest::of(b"uncommitted"),
            };
            summary.summary_sha256 = decode_summary_identity(&summary);
            summary.sink_commit_sha256 = sink_commit_identity(summary.summary_sha256);
            let mut sink = self.sink.take().ok_or(DecodeError::SinkRejected)?;
            let committed = match sink.commit(&summary) {
                Ok(committed) => committed,
                Err(_) => {
                    sink.abort();
                    return Err(DecodeError::SinkRejected);
                }
            };
            if committed != summary.sink_commit_sha256 {
                sink.abort();
                return Err(DecodeError::SinkCommitMismatch);
            }
            Ok((summary, sink))
        })();
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.abort_sink();
                Err(self.failure(error))
            }
        }
    }

    fn abort_sink(&mut self) {
        if let Some(sink) = self.sink.as_mut() {
            sink.abort();
        }
    }

    pub(crate) fn actuals(&self) -> DecodeActuals {
        DecodeActuals {
            pcap_bytes_read: self.bytes_seen,
            staged_events: self.staged_events,
            serialized_event_bytes_staged: self.serialized_event_bytes,
            decoded_event_batch_bytes_staged: self.decoded_event_batch_bytes,
        }
    }

    fn failure(&self, error: DecodeError) -> DecodeFailure {
        DecodeFailure {
            error,
            actuals: self.actuals(),
        }
    }

    fn push_inner(&mut self, bytes: &[u8]) -> Result<(), DecodeError> {
        if bytes.len() > self.limits.max_stream_chunk_bytes {
            return Err(DecodeError::ChunkTooLarge);
        }
        let increment = u64::try_from(bytes.len()).map_err(|_| DecodeError::PcapLengthMismatch)?;
        let next = self
            .bytes_seen
            .checked_add(increment)
            .ok_or(DecodeError::PcapLengthMismatch)?;
        if next > self.expected_pcap_bytes {
            return Err(DecodeError::PcapLengthMismatch);
        }
        self.buffer
            .try_reserve(bytes.len())
            .map_err(map_reserve_error)?;
        self.buffer.extend_from_slice(bytes);
        self.pcap_hasher.update(bytes);
        self.bytes_seen = next;
        self.process_buffer()
    }

    fn process_buffer(&mut self) -> Result<(), DecodeError> {
        if self.pcap_format.is_none() {
            if self.buffer.len() < PCAP_GLOBAL_HEADER_BYTES {
                return Ok(());
            }
            self.pcap_format = Some(parse_global_header(
                &self.buffer[..PCAP_GLOBAL_HEADER_BYTES],
            )?);
            self.buffer.drain(..PCAP_GLOBAL_HEADER_BYTES);
        }

        loop {
            if self.buffer.len() < PCAP_RECORD_HEADER_BYTES {
                return Ok(());
            }
            let format = self.pcap_format.ok_or(DecodeError::InvalidPcapHeader)?;
            let record = parse_record_header(
                &self.buffer[..PCAP_RECORD_HEADER_BYTES],
                format,
                self.limits,
            )?;
            let packet_bytes =
                usize::try_from(record.included_length).map_err(|_| DecodeError::PacketTooLarge)?;
            let record_bytes = PCAP_RECORD_HEADER_BYTES
                .checked_add(packet_bytes)
                .ok_or(DecodeError::PacketTooLarge)?;
            if self.buffer.len() < record_bytes {
                return Ok(());
            }
            if self.packets >= self.limits.max_packets {
                return Err(DecodeError::PacketLimit);
            }
            if self
                .previous_capture_time
                .is_some_and(|previous| record.capture_time_unix_nanos < previous)
            {
                return Err(DecodeError::CaptureClockRegression);
            }
            let capture_time_i64 = i64::try_from(record.capture_time_unix_nanos)
                .map_err(|_| DecodeError::InvalidCaptureTimestamp)?;
            validate_timestamp(capture_time_i64, self.trade_date)
                .map_err(|_| DecodeError::InvalidCaptureTimestamp)?;
            let packet = &self.buffer[PCAP_RECORD_HEADER_BYTES..record_bytes];
            let sink = self.sink.as_mut().ok_or(DecodeError::SinkRejected)?;
            let outcome = decode_packet(
                PacketContext {
                    trade_date: self.trade_date,
                    feed: self.feed,
                    feed_version: self.feed_version,
                    transport_version: self.transport_version,
                    source_file_identity: self.source_file_identity,
                    decoder_contract_sha256: self.contract.contract_sha256(),
                    decode_attempt_evidence_sha256: self.attempt.evidence_sha256,
                    channel_contract: self.contract.channel_contract(),
                    capture_time_unix_nanos: record.capture_time_unix_nanos,
                    message_limit: self.limits.max_messages,
                    decoded_event_batch_limit: self.limits.max_decoded_event_batch_bytes,
                    timestamp_key_limit: self.limits.max_timestamp_keys,
                    send_capture_skew_limit: self.limits.max_send_capture_skew_nanos,
                },
                packet,
                &mut self.channels,
                &mut self.messages,
                &mut self.unmapped_messages,
                &mut self.first_source_time,
                &mut self.last_source_time,
                &mut self.source_clock_messages,
                &mut self.provider_timestamps,
                &mut self.serialized_event_bytes,
                &mut self.decoded_event_batch_bytes,
                &mut self.staged_events,
                &mut self.serialized_events_hasher,
                sink,
            )?;
            self.first_capture_time
                .get_or_insert(record.capture_time_unix_nanos);
            self.previous_capture_time = Some(record.capture_time_unix_nanos);
            self.first_send_time.get_or_insert(outcome.send_time);
            self.last_send_time = Some(outcome.send_time);
            self.max_observed_send_capture_skew_nanos = self
                .max_observed_send_capture_skew_nanos
                .max(outcome.send_capture_skew_nanos);
            self.packets = self
                .packets
                .checked_add(1)
                .ok_or(DecodeError::PacketLimit)?;
            if outcome.non_heartbeat {
                self.segments = self
                    .segments
                    .checked_add(1)
                    .ok_or(DecodeError::PacketLimit)?;
            }
            self.buffer.drain(..record_bytes);
        }
    }
}

impl<S: IexEventSink> Drop for PcapStreamDecoder<S> {
    fn drop(&mut self) {
        self.abort_sink();
    }
}

fn map_reserve_error(_: TryReserveError) -> DecodeError {
    DecodeError::Capacity
}

#[derive(Clone, Copy, Debug)]
enum Endian {
    Little,
    Big,
}

#[derive(Clone, Copy, Debug)]
enum TimestampResolution {
    Micros,
    Nanos,
}

#[derive(Clone, Copy, Debug)]
struct PcapFormat {
    endian: Endian,
    resolution: TimestampResolution,
    snaplen: u32,
}

fn parse_global_header(bytes: &[u8]) -> Result<PcapFormat, DecodeError> {
    let (endian, resolution) = match bytes.get(0..4) {
        Some([0xd4, 0xc3, 0xb2, 0xa1]) => (Endian::Little, TimestampResolution::Micros),
        Some([0x4d, 0x3c, 0xb2, 0xa1]) => (Endian::Little, TimestampResolution::Nanos),
        Some([0xa1, 0xb2, 0xc3, 0xd4]) => (Endian::Big, TimestampResolution::Micros),
        Some([0xa1, 0xb2, 0x3c, 0x4d]) => (Endian::Big, TimestampResolution::Nanos),
        _ => return Err(DecodeError::UnsupportedPcap),
    };
    if read_u16(&bytes[4..6], endian) != 2
        || read_u16(&bytes[6..8], endian) != 4
        || read_i32(&bytes[8..12], endian) != 0
        || read_u32(&bytes[12..16], endian) != 0
    {
        return Err(DecodeError::InvalidPcapHeader);
    }
    let snaplen = read_u32(&bytes[16..20], endian);
    let link_type = read_u32(&bytes[20..24], endian);
    if !(64..=65_535).contains(&snaplen) || link_type != 1 {
        return Err(DecodeError::UnsupportedPcap);
    }
    Ok(PcapFormat {
        endian,
        resolution,
        snaplen,
    })
}

#[derive(Clone, Copy, Debug)]
struct PcapRecord {
    included_length: u32,
    capture_time_unix_nanos: u64,
}

fn parse_record_header(
    bytes: &[u8],
    format: PcapFormat,
    limits: DecodeLimits,
) -> Result<PcapRecord, DecodeError> {
    let seconds = read_u32(&bytes[0..4], format.endian);
    let subsecond = read_u32(&bytes[4..8], format.endian);
    let included_length = read_u32(&bytes[8..12], format.endian);
    let original_length = read_u32(&bytes[12..16], format.endian);
    let subsecond_nanos = match format.resolution {
        TimestampResolution::Micros if subsecond < 1_000_000 => u64::from(subsecond) * 1_000,
        TimestampResolution::Nanos if subsecond < 1_000_000_000 => u64::from(subsecond),
        TimestampResolution::Micros | TimestampResolution::Nanos => {
            return Err(DecodeError::InvalidCaptureTimestamp);
        }
    };
    if included_length != original_length
        || included_length < 64
        || included_length > format.snaplen
        || included_length > limits.max_packet_bytes
    {
        return Err(DecodeError::TruncatedPacket);
    }
    let capture_time_unix_nanos = u64::from(seconds)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(subsecond_nanos))
        .ok_or(DecodeError::InvalidCaptureTimestamp)?;
    Ok(PcapRecord {
        included_length,
        capture_time_unix_nanos,
    })
}

#[derive(Clone, Copy)]
struct PacketContext {
    trade_date: TradeDate,
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    source_file_identity: Sha256Digest,
    decoder_contract_sha256: Sha256Digest,
    decode_attempt_evidence_sha256: Sha256Digest,
    channel_contract: DecodeChannelContract,
    capture_time_unix_nanos: u64,
    message_limit: u64,
    decoded_event_batch_limit: u64,
    timestamp_key_limit: u32,
    send_capture_skew_limit: u64,
}

#[derive(Clone, Copy, Debug)]
struct Continuity {
    session_id: u32,
    next_sequence: i64,
    next_stream_offset: i64,
    last_send_time: EpochNanos,
}

#[derive(Clone, Copy, Debug)]
struct ChannelState {
    continuity: Continuity,
    saw_non_heartbeat: bool,
    saw_start_messages: bool,
    saw_end_messages: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProviderTimestampKey {
    message_type: u8,
    symbol: [u8; 8],
}

#[derive(Clone, Copy, Debug)]
struct ProviderTimestampEntry {
    key: ProviderTimestampKey,
    last_source_time: EpochNanos,
}

#[derive(Clone, Copy, Debug)]
struct PacketOutcome {
    non_heartbeat: bool,
    send_time: EpochNanos,
    send_capture_skew_nanos: u64,
}

#[allow(
    clippy::too_many_arguments,
    reason = "packet decode updates one cohesive continuity/accounting boundary"
)]
fn decode_packet<S: IexEventSink>(
    context: PacketContext,
    packet: &[u8],
    channels: &mut [Option<ChannelState>; MAX_DEEP_PLUS_CHANNELS],
    messages: &mut u64,
    unmapped_messages: &mut u64,
    first_source_time: &mut Option<EpochNanos>,
    last_source_time: &mut Option<EpochNanos>,
    source_clock_messages: &mut u64,
    provider_timestamps: &mut Vec<ProviderTimestampEntry>,
    serialized_event_bytes: &mut u64,
    decoded_event_batch_bytes: &mut u64,
    staged_events: &mut u64,
    serialized_events_hasher: &mut Sha256,
    sink: &mut S,
) -> Result<PacketOutcome, DecodeError> {
    let udp_payload = extract_udp_payload(packet)?;
    if udp_payload.len() < IEX_TP_HEADER_BYTES {
        return Err(DecodeError::TruncatedTransport);
    }
    let header = parse_transport_header(&udp_payload[..IEX_TP_HEADER_BYTES], context)?;
    let send_time = u64::try_from(header.send_time.value())
        .map_err(|_| DecodeError::InvalidTimestamp)?;
    let send_capture_skew_nanos = send_time.abs_diff(context.capture_time_unix_nanos);
    if send_capture_skew_nanos > context.send_capture_skew_limit {
        return Err(DecodeError::SendCaptureClockSkew {
            channel_id: header.channel_id,
            send_time_unix_nanos: send_time,
            capture_time_unix_nanos: context.capture_time_unix_nanos,
            observed_skew_nanos: send_capture_skew_nanos,
            admitted_skew_nanos: context.send_capture_skew_limit,
        });
    }
    let payload = &udp_payload[IEX_TP_HEADER_BYTES..];
    if payload.len() != usize::from(header.payload_length) {
        return Err(DecodeError::MalformedTransportLength);
    }
    let heartbeat = header.payload_length == 0 && header.message_count == 0;
    if (header.payload_length == 0) != (header.message_count == 0) {
        return Err(DecodeError::MalformedTransportLength);
    }

    let channel_index = channel_index(context.feed, header.channel_id)?;
    let channel_role = channel_role(context.channel_contract, header.channel_id)?;
    let current = channels[channel_index];
    validate_continuity(current.map(|state| state.continuity), header, heartbeat)?;
    if heartbeat {
        let prior = current.unwrap_or(ChannelState {
            continuity: Continuity {
                session_id: header.session_id,
                next_sequence: header.first_sequence,
                next_stream_offset: header.stream_offset,
                last_send_time: header.send_time,
            },
            saw_non_heartbeat: false,
            saw_start_messages: false,
            saw_end_messages: false,
        });
        channels[channel_index] = Some(ChannelState {
            continuity: Continuity {
                last_send_time: header.send_time,
                ..prior.continuity
            },
            ..prior
        });
        return Ok(PacketOutcome {
            non_heartbeat: false,
            send_time: header.send_time,
            send_capture_skew_nanos,
        });
    }
    if channel_role == DecodeChannelRole::ReservedHeartbeatOnly {
        return Err(DecodeError::ReservedChannelPayload {
            channel_id: header.channel_id,
        });
    }

    let segment_count = usize::from(header.message_count);
    if segment_count > MAX_MESSAGES_PER_SEGMENT {
        return Err(DecodeError::MessageLimit);
    }
    let next_total = messages
        .checked_add(u64::from(header.message_count))
        .ok_or(DecodeError::MessageLimit)?;
    if next_total > context.message_limit {
        return Err(DecodeError::MessageLimit);
    }
    let mut decoded = Vec::new();
    decoded
        .try_reserve(segment_count)
        .map_err(|_| DecodeError::Capacity)?;
    let mut cursor = 0_usize;
    let mut local_start = current.is_some_and(|state| state.saw_start_messages);
    let mut local_end = current.is_some_and(|state| state.saw_end_messages);
    let mut local_unmapped = 0_u64;

    for index in 0..segment_count {
        let length_end = cursor
            .checked_add(2)
            .ok_or(DecodeError::MalformedMessageLength)?;
        let length_bytes = payload
            .get(cursor..length_end)
            .ok_or(DecodeError::TruncatedMessage)?;
        let message_length = usize::from(u16::from_le_bytes(
            length_bytes
                .try_into()
                .map_err(|_| DecodeError::TruncatedMessage)?,
        ));
        if message_length == 0 || message_length > MAX_SUPPORTED_MESSAGE_BYTES {
            return Err(DecodeError::MalformedMessageLength);
        }
        let message_start = length_end;
        let message_end = message_start
            .checked_add(message_length)
            .ok_or(DecodeError::MalformedMessageLength)?;
        let message = payload
            .get(message_start..message_end)
            .ok_or(DecodeError::TruncatedMessage)?;
        let sequence = header
            .first_sequence
            .checked_add(i64::try_from(index).map_err(|_| DecodeError::SequenceOverflow)?)
            .ok_or(DecodeError::SequenceOverflow)?;
        let stream_offset = header
            .stream_offset
            .checked_add(i64::try_from(cursor).map_err(|_| DecodeError::StreamOffsetOverflow)?)
            .ok_or(DecodeError::StreamOffsetOverflow)?;
        let parsed = parse_message(context.feed, context.trade_date, header.send_time, message)?;
        enforce_session_markers(&parsed.event, &mut local_start, &mut local_end, sequence)?;
        if matches!(parsed.event, IexEvent::Unmapped { .. }) {
            local_unmapped = local_unmapped
                .checked_add(1)
                .ok_or(DecodeError::MessageLimit)?;
        }
        decoded.push(DecodedIexEvent {
            semantics: IexVenueSemantics,
            trade_date: context.trade_date,
            feed: context.feed,
            feed_version: context.feed_version,
            transport_version: context.transport_version,
            source_file_identity: context.source_file_identity,
            decoder_contract_sha256: context.decoder_contract_sha256,
            decode_attempt_evidence_sha256: context.decode_attempt_evidence_sha256,
            channel_id: header.channel_id,
            session_id: header.session_id,
            sequence,
            stream_offset,
            send_time: header.send_time,
            capture_time_unix_nanos: context.capture_time_unix_nanos,
            message_data_bytes: u16::try_from(message_length)
                .map_err(|_| DecodeError::MalformedMessageLength)?,
            message_data_sha256: Sha256Digest::of(message),
            mapped_prefix_bytes: parsed.mapped_prefix_bytes,
            event: parsed.event,
        });
        cursor = message_end;
    }
    if cursor != payload.len() {
        return Err(DecodeError::MalformedTransportLength);
    }
    let mut local_first_source_time = *first_source_time;
    let mut local_last_source_time = *last_source_time;
    let mut local_source_clock_messages = *source_clock_messages;
    for event in &decoded {
        if let Some(source_time) = event_source_time(&event.event) {
            local_first_source_time.get_or_insert(source_time);
            local_last_source_time = Some(source_time);
            local_source_clock_messages = local_source_clock_messages
                .checked_add(1)
                .ok_or(DecodeError::MessageLimit)?;
        }
        if let Some((key, source_time)) = provider_timestamp_coordinate(event)? {
            observe_provider_timestamp(
                provider_timestamps,
                context.timestamp_key_limit,
                key,
                source_time,
            )?;
        }
    }
    let next_sequence = header
        .first_sequence
        .checked_add(i64::from(header.message_count))
        .ok_or(DecodeError::SequenceOverflow)?;
    let next_stream_offset = header
        .stream_offset
        .checked_add(i64::from(header.payload_length))
        .ok_or(DecodeError::StreamOffsetOverflow)?;
    for (index, event) in decoded.into_iter().enumerate() {
        let ordinal = messages
            .checked_add(u64::try_from(index).map_err(|_| DecodeError::MessageLimit)?)
            .ok_or(DecodeError::MessageLimit)?;
        let serialized = serde_json::to_vec(&event).map_err(|_| DecodeError::Serialization)?;
        let serialized_len = u64::try_from(serialized.len()).map_err(|_| DecodeError::Capacity)?;
        let next_serialized_bytes = serialized_event_bytes
            .checked_add(serialized_len)
            .ok_or(DecodeError::Capacity)?;
        let next_batch_bytes = decoded_event_batch_bytes
            .checked_add(SERIALIZED_EVENT_FRAME_BYTES)
            .and_then(|bytes| bytes.checked_add(serialized_len))
            .ok_or(DecodeError::DecodedEventBatchBytesExceeded)?;
        if next_batch_bytes > context.decoded_event_batch_limit {
            return Err(DecodeError::DecodedEventBatchBytesExceeded);
        }
        sink.stage(ordinal, &serialized)
            .map_err(|_| DecodeError::SinkRejected)?;
        serialized_events_hasher.update(ordinal.to_le_bytes());
        serialized_events_hasher.update(serialized_len.to_le_bytes());
        serialized_events_hasher.update(&serialized);
        *serialized_event_bytes = next_serialized_bytes;
        *decoded_event_batch_bytes = next_batch_bytes;
        *staged_events = staged_events
            .checked_add(1)
            .ok_or(DecodeError::MessageLimit)?;
    }
    channels[channel_index] = Some(ChannelState {
        continuity: Continuity {
            session_id: header.session_id,
            next_sequence,
            next_stream_offset,
            last_send_time: header.send_time,
        },
        saw_non_heartbeat: true,
        saw_start_messages: local_start,
        saw_end_messages: local_end,
    });
    *messages = next_total;
    *unmapped_messages = unmapped_messages
        .checked_add(local_unmapped)
        .ok_or(DecodeError::MessageLimit)?;
    *first_source_time = local_first_source_time;
    *last_source_time = local_last_source_time;
    *source_clock_messages = local_source_clock_messages;
    Ok(PacketOutcome {
        non_heartbeat: true,
        send_time: header.send_time,
        send_capture_skew_nanos,
    })
}

fn channel_index(feed: FeedKind, channel_id: u32) -> Result<usize, DecodeError> {
    let admitted = match feed {
        FeedKind::DeepPlusDplc => (1..=16).contains(&channel_id),
        FeedKind::Tops | FeedKind::Deep | FeedKind::DeepPlusDpls => channel_id == 1,
    };
    if !admitted {
        return Err(DecodeError::WrongFeedOrChannel);
    }
    usize::try_from(channel_id - 1).map_err(|_| DecodeError::WrongFeedOrChannel)
}

fn channel_role(
    contract: DecodeChannelContract,
    channel_id: u32,
) -> Result<DecodeChannelRole, DecodeError> {
    match contract {
        DecodeChannelContract::SingleActiveChannelOne if channel_id == 1 => {
            Ok(DecodeChannelRole::Active)
        }
        DecodeChannelContract::Dplc16(distribution) => distribution
            .role(channel_id)
            .ok_or(DecodeError::WrongFeedOrChannel),
        DecodeChannelContract::SingleActiveChannelOne => Err(DecodeError::WrongFeedOrChannel),
    }
}

fn extract_udp_payload(packet: &[u8]) -> Result<&[u8], DecodeError> {
    if packet.len() < ETHERNET_HEADER_BYTES + IPV4_HEADER_BYTES + UDP_HEADER_BYTES {
        return Err(DecodeError::TruncatedPacket);
    }
    let mut ip_start = ETHERNET_HEADER_BYTES;
    let mut ether_type = u16::from_be_bytes([packet[12], packet[13]]);
    let mut vlan_count = 0_u8;
    while matches!(ether_type, 0x8100 | 0x88a8) {
        vlan_count = vlan_count
            .checked_add(1)
            .ok_or(DecodeError::UnsupportedPacket)?;
        if vlan_count > 2 || packet.len() < ip_start + 4 {
            return Err(DecodeError::UnsupportedPacket);
        }
        ether_type = u16::from_be_bytes([packet[ip_start + 2], packet[ip_start + 3]]);
        ip_start += 4;
    }
    if ether_type != 0x0800 || packet.len() < ip_start + IPV4_HEADER_BYTES {
        return Err(DecodeError::UnsupportedPacket);
    }
    let version_ihl = packet[ip_start];
    if version_ihl != 0x45 {
        return Err(DecodeError::UnsupportedPacket);
    }
    let ip_total_length = usize::from(u16::from_be_bytes([
        packet[ip_start + 2],
        packet[ip_start + 3],
    ]));
    let ip_end = ip_start
        .checked_add(ip_total_length)
        .ok_or(DecodeError::TruncatedPacket)?;
    if ip_total_length < IPV4_HEADER_BYTES + UDP_HEADER_BYTES || ip_end > packet.len() {
        return Err(DecodeError::TruncatedPacket);
    }
    let flags_and_offset = u16::from_be_bytes([packet[ip_start + 6], packet[ip_start + 7]]);
    if flags_and_offset & 0xbfff != 0 || packet[ip_start + 9] != 17 {
        return Err(DecodeError::UnsupportedPacket);
    }
    let ip_header = &packet[ip_start..ip_start + IPV4_HEADER_BYTES];
    if checksum_sum(&[ip_header]) != 0xffff {
        return Err(DecodeError::InvalidIpv4Checksum);
    }
    let udp_start = ip_start + IPV4_HEADER_BYTES;
    let udp_length = usize::from(u16::from_be_bytes([
        packet[udp_start + 4],
        packet[udp_start + 5],
    ]));
    if udp_length < UDP_HEADER_BYTES || udp_start + udp_length != ip_end {
        return Err(DecodeError::MalformedUdpLength);
    }
    let udp = &packet[udp_start..ip_end];
    let udp_checksum = u16::from_be_bytes([packet[udp_start + 6], packet[udp_start + 7]]);
    if udp_checksum != 0 {
        let pseudo = [
            packet[ip_start + 12],
            packet[ip_start + 13],
            packet[ip_start + 14],
            packet[ip_start + 15],
            packet[ip_start + 16],
            packet[ip_start + 17],
            packet[ip_start + 18],
            packet[ip_start + 19],
            0,
            17,
            packet[udp_start + 4],
            packet[udp_start + 5],
        ];
        if checksum_sum(&[&pseudo, udp]) != 0xffff {
            return Err(DecodeError::InvalidUdpChecksum);
        }
    }
    Ok(&udp[UDP_HEADER_BYTES..])
}

fn checksum_sum(parts: &[&[u8]]) -> u16 {
    let mut sum = 0_u32;
    let mut pending = None;
    for part in parts {
        for &byte in *part {
            if let Some(high) = pending.take() {
                sum = sum.wrapping_add(u32::from(u16::from_be_bytes([high, byte])));
            } else {
                pending = Some(byte);
            }
        }
    }
    if let Some(high) = pending {
        sum = sum.wrapping_add(u32::from(u16::from_be_bytes([high, 0])));
    }
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    u16::try_from(sum).unwrap_or(0)
}

#[derive(Clone, Copy, Debug)]
struct TransportHeader {
    channel_id: u32,
    session_id: u32,
    payload_length: u16,
    message_count: u16,
    stream_offset: i64,
    first_sequence: i64,
    send_time: EpochNanos,
}

fn parse_transport_header(
    bytes: &[u8],
    context: PacketContext,
) -> Result<TransportHeader, DecodeError> {
    if bytes[0] != 1 || bytes[1] != 0 {
        return Err(DecodeError::UnsupportedVersion);
    }
    let protocol_id = u16::from_le_bytes([bytes[2], bytes[3]]);
    let channel_id = u32::from_le_bytes(
        bytes[4..8]
            .try_into()
            .map_err(|_| DecodeError::TruncatedTransport)?,
    );
    let session_id = u32::from_le_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| DecodeError::TruncatedTransport)?,
    );
    if protocol_id != context.feed.protocol_id()
        || channel_index(context.feed, channel_id).is_err()
        || session_id == 0
    {
        return Err(DecodeError::WrongFeedOrChannel);
    }
    let payload_length = u16::from_le_bytes([bytes[12], bytes[13]]);
    let message_count = u16::from_le_bytes([bytes[14], bytes[15]]);
    let stream_offset = i64::from_le_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| DecodeError::TruncatedTransport)?,
    );
    let first_sequence = i64::from_le_bytes(
        bytes[24..32]
            .try_into()
            .map_err(|_| DecodeError::TruncatedTransport)?,
    );
    if stream_offset < 0 || first_sequence <= 0 {
        return Err(DecodeError::InvalidContinuityCoordinate);
    }
    let send_time_raw = i64::from_le_bytes(
        bytes[32..40]
            .try_into()
            .map_err(|_| DecodeError::TruncatedTransport)?,
    );
    let send_time = validate_timestamp(send_time_raw, context.trade_date)?;
    Ok(TransportHeader {
        channel_id,
        session_id,
        payload_length,
        message_count,
        stream_offset,
        first_sequence,
        send_time,
    })
}

fn validate_continuity(
    current: Option<Continuity>,
    header: TransportHeader,
    heartbeat: bool,
) -> Result<(), DecodeError> {
    let Some(current) = current else {
        if header.first_sequence != 1 || header.stream_offset != 0 {
            return Err(DecodeError::CaptureStartsMidSession);
        }
        return Ok(());
    };
    if current.session_id != header.session_id {
        return Err(DecodeError::SessionReset);
    }
    if header.send_time < current.last_send_time {
        return Err(DecodeError::SendClockRegression);
    }
    if header.first_sequence != current.next_sequence {
        return if header.first_sequence > current.next_sequence {
            Err(DecodeError::SequenceGap {
                expected: current.next_sequence,
                actual: header.first_sequence,
            })
        } else {
            Err(DecodeError::DuplicateOrOutOfOrderSequence)
        };
    }
    if header.stream_offset != current.next_stream_offset {
        return if header.stream_offset > current.next_stream_offset {
            Err(DecodeError::StreamOffsetGap {
                expected: current.next_stream_offset,
                actual: header.stream_offset,
            })
        } else {
            Err(DecodeError::DuplicateOrOutOfOrderOffset)
        };
    }
    if heartbeat && (header.payload_length != 0 || header.message_count != 0) {
        return Err(DecodeError::MalformedTransportLength);
    }
    Ok(())
}

struct ParsedMessage {
    event: IexEvent,
    mapped_prefix_bytes: u16,
}

fn parse_message(
    feed: FeedKind,
    trade_date: TradeDate,
    send_time: EpochNanos,
    bytes: &[u8],
) -> Result<ParsedMessage, DecodeError> {
    let message_type = *bytes.first().ok_or(DecodeError::TruncatedMessage)?;
    let parsed = match (feed, message_type) {
        (_, b'S') => parse_system(bytes, trade_date)?,
        (_, b'D') => parse_security_directory(bytes, trade_date)?,
        (FeedKind::Deep | FeedKind::DeepPlusDpls | FeedKind::DeepPlusDplc, b'E') => {
            parse_security_event(bytes, trade_date)?
        }
        (_, b'H') => parse_trading_status(bytes, trade_date)?,
        (_, b'I') => parse_retail_liquidity(bytes, trade_date)?,
        (_, b'O') => parse_operational_halt(bytes, trade_date)?,
        (_, b'P') => parse_short_sale_price_test(bytes, trade_date)?,
        (FeedKind::Tops, b'Q') => parse_quote(bytes, trade_date)?,
        (FeedKind::Deep, b'8') => parse_price_level(bytes, trade_date, PriceLevelSide::Buy)?,
        (FeedKind::Deep, b'5') => parse_price_level(bytes, trade_date, PriceLevelSide::Sell)?,
        (_, b'T') => parse_trade(bytes, trade_date, false)?,
        (_, b'B') => parse_trade(bytes, trade_date, true)?,
        (FeedKind::Tops | FeedKind::Deep, b'X') => parse_official_price(bytes, trade_date)?,
        (FeedKind::Tops | FeedKind::Deep, b'A') => parse_auction(bytes, trade_date)?,
        (FeedKind::DeepPlusDpls | FeedKind::DeepPlusDplc, b'a') => {
            parse_add_order(bytes, trade_date)?
        }
        (FeedKind::DeepPlusDpls | FeedKind::DeepPlusDplc, b'M') => {
            parse_modify_order(bytes, trade_date)?
        }
        (FeedKind::DeepPlusDpls | FeedKind::DeepPlusDplc, b'R') => {
            parse_delete_order(bytes, trade_date)?
        }
        (FeedKind::DeepPlusDpls | FeedKind::DeepPlusDplc, b'L') => {
            parse_execute_order(bytes, trade_date)?
        }
        (FeedKind::DeepPlusDpls | FeedKind::DeepPlusDplc, b'C') => {
            parse_clear_book(bytes, trade_date)?
        }
        _ => ParsedMessage {
            event: IexEvent::Unmapped {
                message_type,
                byte_len: u16::try_from(bytes.len())
                    .map_err(|_| DecodeError::MalformedMessageLength)?,
                message_sha256: Sha256Digest::of(bytes),
            },
            mapped_prefix_bytes: 0,
        },
    };
    if event_source_time(&parsed.event).is_some_and(|source_time| source_time > send_time) {
        return Err(DecodeError::EventAfterSendTime);
    }
    Ok(parsed)
}

fn parse_system(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 10)?;
    let event = match bytes[1] {
        b'O' => SystemEventCode::StartMessages,
        b'S' => SystemEventCode::StartSystemHours,
        b'R' => SystemEventCode::StartRegularMarket,
        b'M' => SystemEventCode::EndRegularMarket,
        b'E' => SystemEventCode::EndSystemHours,
        b'C' => SystemEventCode::EndMessages,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    Ok(ParsedMessage {
        event: IexEvent::System {
            event,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 10,
    })
}

fn parse_security_directory(
    bytes: &[u8],
    trade_date: TradeDate,
) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 31)?;
    if bytes[1] & 0x1f != 0 {
        return Err(DecodeError::InvalidMessageValue);
    }
    let round_lot_size = read_le_u32(&bytes[18..22])?;
    if round_lot_size == 0 {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    let luld_tier = match bytes[30] {
        0 => LuldTier::NotApplicable,
        1 => LuldTier::Tier1,
        2 => LuldTier::Tier2,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    Ok(ParsedMessage {
        event: IexEvent::SecurityDirectory {
            flags: bytes[1],
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            round_lot_size,
            adjusted_poc_price: PriceUnits1e4::try_new(read_le_i64(&bytes[22..30])?)
                .map_err(map_model)?,
            luld_tier,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 31,
    })
}

fn parse_security_event(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 18)?;
    let event = match bytes[1] {
        b'O' => SecurityEventCode::OpeningProcessComplete,
        b'C' => SecurityEventCode::ClosingProcessComplete,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    Ok(ParsedMessage {
        event: IexEvent::SecurityEvent {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            event,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 18,
    })
}

fn parse_trading_status(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 22)?;
    let status = match bytes[1] {
        b'H' => TradingStatus::Halted,
        b'O' => TradingStatus::OrderAcceptance,
        b'P' => TradingStatus::Paused,
        b'T' => TradingStatus::Trading,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    Ok(ParsedMessage {
        event: IexEvent::TradingStatus {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            status,
            reason: parse_fixed_ascii(&bytes[18..22], true)?,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 22,
    })
}

fn parse_retail_liquidity(
    bytes: &[u8],
    trade_date: TradeDate,
) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 18)?;
    let indicator = match bytes[1] {
        b' ' => RetailLiquidityIndicator::NotApplicable,
        b'A' => RetailLiquidityIndicator::Buy,
        b'B' => RetailLiquidityIndicator::Sell,
        b'C' => RetailLiquidityIndicator::BuyAndSell,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    Ok(ParsedMessage {
        event: IexEvent::RetailLiquidity {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            indicator,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 18,
    })
}

fn parse_operational_halt(
    bytes: &[u8],
    trade_date: TradeDate,
) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 18)?;
    let status = match bytes[1] {
        b'O' => OperationalHaltStatus::Halted,
        b'N' => OperationalHaltStatus::NotHalted,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    Ok(ParsedMessage {
        event: IexEvent::OperationalHalt {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            status,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 18,
    })
}

fn parse_short_sale_price_test(
    bytes: &[u8],
    trade_date: TradeDate,
) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 19)?;
    let in_effect = match bytes[1] {
        0 => false,
        1 => true,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    let detail = match bytes[18] {
        b' ' => ShortSalePriceTestDetail::NoPriceTest,
        b'A' => ShortSalePriceTestDetail::Activated,
        b'C' => ShortSalePriceTestDetail::Continued,
        b'D' => ShortSalePriceTestDetail::Deactivated,
        b'N' => ShortSalePriceTestDetail::NotAvailable,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    if (in_effect
        && matches!(
            detail,
            ShortSalePriceTestDetail::NoPriceTest | ShortSalePriceTestDetail::Deactivated
        ))
        || (!in_effect
            && matches!(
                detail,
                ShortSalePriceTestDetail::Activated | ShortSalePriceTestDetail::Continued
            ))
    {
        return Err(DecodeError::InvalidMessageValue);
    }
    Ok(ParsedMessage {
        event: IexEvent::ShortSalePriceTest {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            in_effect,
            detail,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 19,
    })
}

fn parse_quote(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 42)?;
    let bid_size = read_le_u32(&bytes[18..22])?;
    let bid_price = PriceUnits1e4::try_new(read_le_i64(&bytes[22..30])?).map_err(map_model)?;
    let ask_price = PriceUnits1e4::try_new(read_le_i64(&bytes[30..38])?).map_err(map_model)?;
    let ask_size = read_le_u32(&bytes[38..42])?;
    if (bid_size == 0) != (bid_price.value() == 0)
        || (ask_size == 0) != (ask_price.value() == 0)
        || (bid_price.value() > 0 && ask_price.value() > 0 && bid_price.value() > ask_price.value())
    {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    Ok(ParsedMessage {
        event: IexEvent::Quote {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            flags: bytes[1],
            bid_size,
            bid_price,
            ask_price,
            ask_size,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 42,
    })
}

fn parse_price_level(
    bytes: &[u8],
    trade_date: TradeDate,
    side: PriceLevelSide,
) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 30)?;
    let event_complete = match bytes[1] {
        0 => false,
        1 => true,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    let price = PriceUnits1e4::try_new(read_le_i64(&bytes[22..30])?).map_err(map_model)?;
    if price.value() == 0 {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    Ok(ParsedMessage {
        event: IexEvent::PriceLevel {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            side,
            event_complete,
            size: read_le_u32(&bytes[18..22])?,
            price,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 30,
    })
}

fn parse_trade(
    bytes: &[u8],
    trade_date: TradeDate,
    is_break: bool,
) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 38)?;
    let size = read_le_u32(&bytes[18..22])?;
    let price = PriceUnits1e4::try_new(read_le_i64(&bytes[22..30])?).map_err(map_model)?;
    let trade_id = read_le_i64(&bytes[30..38])?;
    if size == 0 || price.value() == 0 || trade_id < 0 {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    let symbol = parse_fixed_ascii(&bytes[10..18], false)?;
    let source_time = message_timestamp(bytes, trade_date)?;
    let event = if is_break {
        IexEvent::TradeBreak {
            symbol,
            sale_condition_flags: bytes[1],
            size,
            price,
            trade_id,
            source_time,
        }
    } else {
        IexEvent::Trade {
            symbol,
            sale_condition_flags: bytes[1],
            size,
            price,
            trade_id,
            source_time,
        }
    };
    Ok(ParsedMessage {
        event,
        mapped_prefix_bytes: 38,
    })
}

fn parse_official_price(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 26)?;
    let price_type = match bytes[1] {
        b'Q' => PriceType::Opening,
        b'M' => PriceType::Closing,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    let price = PriceUnits1e4::try_new(read_le_i64(&bytes[18..26])?).map_err(map_model)?;
    if price.value() == 0 {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    Ok(ParsedMessage {
        event: IexEvent::OfficialPrice {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            price_type,
            price,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 26,
    })
}

fn parse_auction(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 80)?;
    let auction_type = match bytes[1] {
        b'O' => AuctionType::Opening,
        b'C' => AuctionType::Closing,
        b'I' => AuctionType::Ipo,
        b'H' => AuctionType::Halt,
        b'V' => AuctionType::Volatility,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    let imbalance_shares = read_le_u32(&bytes[38..42])?;
    let imbalance_side = match bytes[42] {
        b'B' => AuctionImbalanceSide::Buy,
        b'S' => AuctionImbalanceSide::Sell,
        b'N' => AuctionImbalanceSide::None,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    if (imbalance_shares == 0) != (imbalance_side == AuctionImbalanceSide::None) {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    let scheduled_auction_time_unix_seconds = read_le_u32(&bytes[44..48])?;
    validate_event_seconds(scheduled_auction_time_unix_seconds, trade_date)?;
    Ok(ParsedMessage {
        event: IexEvent::Auction {
            auction_type,
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            paired_shares: read_le_u32(&bytes[18..22])?,
            reference_price: parse_price(&bytes[22..30])?,
            indicative_clearing_price: parse_price(&bytes[30..38])?,
            imbalance_shares,
            imbalance_side,
            extension_number: bytes[43],
            scheduled_auction_time_unix_seconds,
            auction_book_clearing_price: parse_price(&bytes[48..56])?,
            collar_reference_price: parse_price(&bytes[56..64])?,
            lower_auction_collar: parse_price(&bytes[64..72])?,
            upper_auction_collar: parse_price(&bytes[72..80])?,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 80,
    })
}

fn parse_add_order(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 38)?;
    let side = match bytes[1] {
        b'8' => OrderSide::Buy,
        b'5' => OrderSide::Sell,
        _ => return Err(DecodeError::InvalidMessageValue),
    };
    let order_id = read_le_i64(&bytes[18..26])?;
    let size = read_le_u32(&bytes[26..30])?;
    let price = parse_price(&bytes[30..38])?;
    validate_order_values(order_id, size, price)?;
    Ok(ParsedMessage {
        event: IexEvent::AddOrder {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            side,
            order_id,
            size,
            price,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 38,
    })
}

fn parse_modify_order(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 38)?;
    if bytes[1] & 0xfe != 0 {
        return Err(DecodeError::InvalidMessageValue);
    }
    let order_id = read_le_i64(&bytes[18..26])?;
    let size = read_le_u32(&bytes[26..30])?;
    let price = parse_price(&bytes[30..38])?;
    validate_order_values(order_id, size, price)?;
    Ok(ParsedMessage {
        event: IexEvent::ModifyOrder {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            order_id,
            size,
            price,
            maintains_priority: bytes[1] == 1,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 38,
    })
}

fn parse_delete_order(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 26)?;
    if bytes[1] != 0 {
        return Err(DecodeError::InvalidMessageValue);
    }
    let order_id = read_le_i64(&bytes[18..26])?;
    if order_id < 0 {
        return Err(DecodeError::InvalidMessageValue);
    }
    Ok(ParsedMessage {
        event: IexEvent::DeleteOrder {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            order_id,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 26,
    })
}

fn parse_execute_order(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 46)?;
    let order_id = read_le_i64(&bytes[18..26])?;
    let size = read_le_u32(&bytes[26..30])?;
    let price = parse_price(&bytes[30..38])?;
    let trade_id = read_le_i64(&bytes[38..46])?;
    if order_id < 0 || trade_id < 0 || size == 0 || price.value() == 0 {
        return Err(DecodeError::InvalidPriceOrSize);
    }
    Ok(ParsedMessage {
        event: IexEvent::ExecuteOrder {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            sale_condition_flags: bytes[1],
            order_id,
            size,
            price,
            trade_id,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 46,
    })
}

fn parse_clear_book(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 18)?;
    if bytes[1] != 0 {
        return Err(DecodeError::InvalidMessageValue);
    }
    Ok(ParsedMessage {
        event: IexEvent::ClearBook {
            symbol: parse_fixed_ascii(&bytes[10..18], false)?,
            source_time: message_timestamp(bytes, trade_date)?,
        },
        mapped_prefix_bytes: 18,
    })
}

fn parse_price(bytes: &[u8]) -> Result<PriceUnits1e4, DecodeError> {
    PriceUnits1e4::try_new(read_le_i64(bytes)?).map_err(map_model)
}

fn validate_order_values(
    order_id: i64,
    size: u32,
    price: PriceUnits1e4,
) -> Result<(), DecodeError> {
    if order_id < 0 || size == 0 || price.value() == 0 {
        Err(DecodeError::InvalidPriceOrSize)
    } else {
        Ok(())
    }
}

fn validate_event_seconds(value: u32, trade_date: TradeDate) -> Result<(), DecodeError> {
    let nanos = i64::from(value)
        .checked_mul(1_000_000_000)
        .ok_or(DecodeError::InvalidTimestamp)?;
    validate_timestamp(nanos, trade_date).map(|_| ())
}

fn require_prefix(bytes: &[u8], minimum: usize) -> Result<(), DecodeError> {
    if bytes.len() < minimum {
        Err(DecodeError::TruncatedMessage)
    } else {
        Ok(())
    }
}

fn message_timestamp(bytes: &[u8], trade_date: TradeDate) -> Result<EpochNanos, DecodeError> {
    validate_timestamp(read_le_i64(&bytes[2..10])?, trade_date)
}

fn validate_timestamp(value: i64, trade_date: TradeDate) -> Result<EpochNanos, DecodeError> {
    let timestamp = EpochNanos::try_new(value).map_err(map_model)?;
    let start = trade_date
        .start_epoch_nanos()
        .map_err(|_| DecodeError::InvalidTimestamp)?;
    let end = MAX_TIMESTAMP_HOURS_FROM_TRADE_DATE
        .checked_mul(3_600_000_000_000)
        .and_then(|span| start.checked_add(span))
        .ok_or(DecodeError::InvalidTimestamp)?;
    if value < start || value >= end {
        Err(DecodeError::InvalidTimestamp)
    } else {
        Ok(timestamp)
    }
}

fn parse_fixed_ascii(bytes: &[u8], allow_empty: bool) -> Result<String, DecodeError> {
    let end = bytes
        .iter()
        .position(|&byte| byte == b' ')
        .unwrap_or(bytes.len());
    if bytes[end..].iter().any(|&byte| byte != b' ')
        || (!allow_empty && end == 0)
        || !bytes[..end]
            .iter()
            .all(|byte| byte.is_ascii_graphic() && *byte != b'\"' && *byte != b'\\')
    {
        return Err(DecodeError::InvalidText);
    }
    std::str::from_utf8(&bytes[..end])
        .map(str::to_owned)
        .map_err(|_| DecodeError::InvalidText)
}

fn event_source_time(event: &IexEvent) -> Option<EpochNanos> {
    match event {
        IexEvent::System { source_time, .. }
        | IexEvent::SecurityDirectory { source_time, .. }
        | IexEvent::SecurityEvent { source_time, .. }
        | IexEvent::TradingStatus { source_time, .. }
        | IexEvent::RetailLiquidity { source_time, .. }
        | IexEvent::OperationalHalt { source_time, .. }
        | IexEvent::ShortSalePriceTest { source_time, .. }
        | IexEvent::Quote { source_time, .. }
        | IexEvent::PriceLevel { source_time, .. }
        | IexEvent::Trade { source_time, .. }
        | IexEvent::TradeBreak { source_time, .. }
        | IexEvent::OfficialPrice { source_time, .. }
        | IexEvent::Auction { source_time, .. }
        | IexEvent::AddOrder { source_time, .. }
        | IexEvent::ModifyOrder { source_time, .. }
        | IexEvent::DeleteOrder { source_time, .. }
        | IexEvent::ExecuteOrder { source_time, .. }
        | IexEvent::ClearBook { source_time, .. } => Some(*source_time),
        IexEvent::Unmapped { .. } => None,
    }
}

fn provider_timestamp_coordinate(
    decoded: &DecodedIexEvent,
) -> Result<Option<(ProviderTimestampKey, EpochNanos)>, DecodeError> {
    let Some(source_time) = event_source_time(&decoded.event) else {
        return Ok(None);
    };
    let (message_type, symbol) = match &decoded.event {
        // The provider's monotonic guarantee is defined for a message-type/symbol pairing.
        // System messages have no symbol and are repeated per channel, so no synthetic symbol
        // coordinate is invented for them.
        IexEvent::System { .. } => return Ok(None),
        IexEvent::SecurityDirectory { symbol, .. } => (b'D', Some(symbol.as_str())),
        IexEvent::SecurityEvent { symbol, .. } => (b'E', Some(symbol.as_str())),
        IexEvent::TradingStatus { symbol, .. } => (b'H', Some(symbol.as_str())),
        IexEvent::RetailLiquidity { symbol, .. } => (b'I', Some(symbol.as_str())),
        IexEvent::OperationalHalt { symbol, .. } => (b'O', Some(symbol.as_str())),
        IexEvent::ShortSalePriceTest { symbol, .. } => (b'P', Some(symbol.as_str())),
        IexEvent::Quote { symbol, .. } => (b'Q', Some(symbol.as_str())),
        IexEvent::PriceLevel { symbol, side, .. } => (
            match side {
                PriceLevelSide::Buy => b'8',
                PriceLevelSide::Sell => b'5',
            },
            Some(symbol.as_str()),
        ),
        IexEvent::Trade { symbol, .. } => (b'T', Some(symbol.as_str())),
        IexEvent::TradeBreak { symbol, .. } => (b'B', Some(symbol.as_str())),
        IexEvent::OfficialPrice { symbol, .. } => (b'X', Some(symbol.as_str())),
        IexEvent::Auction { symbol, .. } => (b'A', Some(symbol.as_str())),
        IexEvent::AddOrder { symbol, .. } => (b'a', Some(symbol.as_str())),
        IexEvent::ModifyOrder { symbol, .. } => (b'M', Some(symbol.as_str())),
        IexEvent::DeleteOrder { symbol, .. } => (b'R', Some(symbol.as_str())),
        IexEvent::ExecuteOrder { symbol, .. } => (b'L', Some(symbol.as_str())),
        IexEvent::ClearBook { symbol, .. } => (b'C', Some(symbol.as_str())),
        IexEvent::Unmapped { .. } => return Ok(None),
    };
    let mut symbol_key = [0_u8; 8];
    if let Some(symbol) = symbol {
        if symbol.is_empty() || symbol.len() > symbol_key.len() {
            return Err(DecodeError::InvalidText);
        }
        symbol_key[..symbol.len()].copy_from_slice(symbol.as_bytes());
    }
    Ok(Some((
        ProviderTimestampKey {
            message_type,
            symbol: symbol_key,
        },
        source_time,
    )))
}

fn observe_provider_timestamp(
    entries: &mut Vec<ProviderTimestampEntry>,
    max_keys: u32,
    key: ProviderTimestampKey,
    source_time: EpochNanos,
) -> Result<(), DecodeError> {
    match entries.binary_search_by(|entry| entry.key.cmp(&key)) {
        Ok(index) => {
            let entry = entries
                .get_mut(index)
                .ok_or(DecodeError::ProviderTimestampStateLimit)?;
            if source_time < entry.last_source_time {
                return Err(DecodeError::ProviderTimestampRegression {
                    message_type: key.message_type,
                    symbol: key.symbol,
                    previous_source_time_unix_nanos: entry.last_source_time.value(),
                    actual_source_time_unix_nanos: source_time.value(),
                });
            }
            entry.last_source_time = source_time;
        }
        Err(index) => {
            if u32::try_from(entries.len()).map_or(true, |count| count >= max_keys) {
                return Err(DecodeError::ProviderTimestampStateLimit);
            }
            entries.insert(
                index,
                ProviderTimestampEntry {
                    key,
                    last_source_time: source_time,
                },
            );
        }
    }
    Ok(())
}

fn enforce_session_markers(
    event: &IexEvent,
    saw_start: &mut bool,
    saw_end: &mut bool,
    sequence: i64,
) -> Result<(), DecodeError> {
    if *saw_end {
        return Err(DecodeError::MessageAfterSessionEnd);
    }
    match event {
        IexEvent::System {
            event: SystemEventCode::StartMessages,
            ..
        } => {
            if *saw_start || sequence != 1 {
                return Err(DecodeError::InvalidSessionMarkers);
            }
            *saw_start = true;
        }
        IexEvent::System {
            event: SystemEventCode::EndMessages,
            ..
        } => {
            if !*saw_start {
                return Err(DecodeError::InvalidSessionMarkers);
            }
            *saw_end = true;
        }
        _ if !*saw_start => return Err(DecodeError::InvalidSessionMarkers),
        _ => {}
    }
    Ok(())
}

fn read_le_u32(bytes: &[u8]) -> Result<u32, DecodeError> {
    bytes
        .try_into()
        .map(u32::from_le_bytes)
        .map_err(|_| DecodeError::TruncatedMessage)
}

fn read_le_i64(bytes: &[u8]) -> Result<i64, DecodeError> {
    bytes
        .try_into()
        .map(i64::from_le_bytes)
        .map_err(|_| DecodeError::TruncatedMessage)
}

fn read_u16(bytes: &[u8], endian: Endian) -> u16 {
    let value = [bytes[0], bytes[1]];
    match endian {
        Endian::Little => u16::from_le_bytes(value),
        Endian::Big => u16::from_be_bytes(value),
    }
}

fn read_u32(bytes: &[u8], endian: Endian) -> u32 {
    let value = [bytes[0], bytes[1], bytes[2], bytes[3]];
    match endian {
        Endian::Little => u32::from_le_bytes(value),
        Endian::Big => u32::from_be_bytes(value),
    }
}

fn read_i32(bytes: &[u8], endian: Endian) -> i32 {
    let value = [bytes[0], bytes[1], bytes[2], bytes[3]];
    match endian {
        Endian::Little => i32::from_le_bytes(value),
        Endian::Big => i32::from_be_bytes(value),
    }
}

fn map_model(error: ModelError) -> DecodeError {
    match error {
        ModelError::InvalidTimestamp => DecodeError::InvalidTimestamp,
        ModelError::InvalidPrice => DecodeError::InvalidPriceOrSize,
    }
}

fn terminal_channel_summaries(
    contract: DecodeContract,
    trade_date: TradeDate,
    states: &[Option<ChannelState>; MAX_DEEP_PLUS_CHANNELS],
) -> Result<(Vec<DecodeChannelSummary>, Sha256Digest), DecodeError> {
    contract.validate_for(trade_date)?;
    let mut summaries = Vec::new();
    summaries
        .try_reserve(MAX_DEEP_PLUS_CHANNELS)
        .map_err(|_| DecodeError::Capacity)?;
    for (index, state) in states.iter().enumerate() {
        let channel_id = u32::try_from(index + 1).map_err(|_| DecodeError::IncompleteSession)?;
        let role = match contract.channel_contract() {
            DecodeChannelContract::SingleActiveChannelOne if channel_id == 1 => {
                DecodeChannelRole::Active
            }
            DecodeChannelContract::SingleActiveChannelOne => {
                if state.is_some() {
                    return Err(DecodeError::WrongFeedOrChannel);
                }
                continue;
            }
            DecodeChannelContract::Dplc16(distribution) => distribution
                .role(channel_id)
                .ok_or(DecodeError::WrongFeedOrChannel)?,
        };
        let Some(state) = state else {
            return Err(DecodeError::MissingRequiredChannel { channel_id });
        };
        if state.continuity.session_id == 0
            || state.continuity.next_sequence <= 0
            || state.continuity.next_stream_offset < 0
            || (role == DecodeChannelRole::Active
                && (!state.saw_non_heartbeat
                    || !state.saw_start_messages
                    || !state.saw_end_messages
                    || state.continuity.next_sequence <= 1
                    || state.continuity.next_stream_offset <= 0))
            || (role == DecodeChannelRole::ReservedHeartbeatOnly
                && (state.saw_non_heartbeat
                    || state.saw_start_messages
                    || state.saw_end_messages))
        {
            return Err(DecodeError::IncompleteSession);
        }
        summaries.push(DecodeChannelSummary {
            channel_id,
            session_id: state.continuity.session_id,
            next_sequence: state.continuity.next_sequence,
            next_stream_offset: state.continuity.next_stream_offset,
            last_send_time: state.continuity.last_send_time,
            heartbeat_only: !state.saw_non_heartbeat,
            role,
        });
    }
    if summaries.is_empty() {
        return Err(DecodeError::IncompleteSession);
    }
    let identity = validate_channel_summaries(contract, trade_date, &summaries)?;
    Ok((summaries, identity))
}

fn validate_channel_summaries(
    contract: DecodeContract,
    trade_date: TradeDate,
    summaries: &[DecodeChannelSummary],
) -> Result<Sha256Digest, DecodeError> {
    contract.validate_for(trade_date)?;
    let expected_channels = match contract.channel_contract() {
        DecodeChannelContract::SingleActiveChannelOne => 1,
        DecodeChannelContract::Dplc16(_) => MAX_DEEP_PLUS_CHANNELS,
    };
    if summaries.len() != expected_channels {
        return Err(DecodeError::SummaryIdentityMismatch);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/iex-hist-channel-sessions/v2");
    let mut previous_channel = 0_u32;
    for summary in summaries {
        let expected_role = channel_role(contract.channel_contract(), summary.channel_id)?;
        if summary.channel_id == 0
            || summary.channel_id > 16
            || summary.channel_id <= previous_channel
            || summary.session_id == 0
            || summary.next_sequence <= 0
            || summary.next_stream_offset < 0
            || (!summary.heartbeat_only
                && (summary.next_sequence <= 1 || summary.next_stream_offset <= 0))
            || summary.role != expected_role
            || (summary.role == DecodeChannelRole::Active && summary.heartbeat_only)
            || (summary.role == DecodeChannelRole::ReservedHeartbeatOnly
                && !summary.heartbeat_only)
        {
            return Err(DecodeError::SummaryIdentityMismatch);
        }
        previous_channel = summary.channel_id;
        hasher.update(summary.channel_id.to_le_bytes());
        hasher.update(summary.session_id.to_le_bytes());
        hasher.update(summary.next_sequence.to_le_bytes());
        hasher.update(summary.next_stream_offset.to_le_bytes());
        hasher.update(summary.last_send_time.value().to_le_bytes());
        hasher.update([u8::from(summary.heartbeat_only)]);
        hasher.update([match summary.role {
            DecodeChannelRole::Active => 1,
            DecodeChannelRole::ReservedHeartbeatOnly => 2,
        }]);
    }
    let count = u8::try_from(summaries.len()).map_err(|_| DecodeError::SummaryIdentityMismatch)?;
    hasher.update([count]);
    Ok(Sha256Digest::from_bytes(hasher.finalize().into()))
}

fn sink_commit_identity(summary_sha256: Sha256Digest) -> Sha256Digest {
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-transactional-sink-commit/v2",
        summary_sha256.as_bytes(),
    ])
}

fn decode_summary_identity(summary: &DecodeSummary) -> Sha256Digest {
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-decode-summary/v3",
        summary.capture_receipt_sha256.as_bytes(),
        summary.trade_date.as_bytes(),
        summary.decoder_contract_sha256.as_bytes(),
        summary.decoder_implementation_sha256.as_bytes(),
        summary.decode_limits_sha256.as_bytes(),
        summary.decode_attempt_evidence_sha256.as_bytes(),
        summary.decode_attempt_sha256.as_bytes(),
        summary.decode_request_sha256.as_bytes(),
        summary.decode_reservation_sha256.as_bytes(),
        &summary.decode_authority_generation.to_le_bytes(),
        summary.decode_storage_root_sha256.as_bytes(),
        &summary.decode_admitted_at_unix_nanos.to_le_bytes(),
        &summary.decode_admitted_utc_offset_seconds.to_le_bytes(),
        summary.decode_admitted_observed_date.as_bytes(),
        &summary.decode_deadline_unix_nanos.to_le_bytes(),
        &summary.pcap_bytes.to_le_bytes(),
        &summary.packets.to_le_bytes(),
        &summary.segments.to_le_bytes(),
        &summary.messages.to_le_bytes(),
        &summary.unmapped_messages.to_le_bytes(),
        &[summary.channels],
        summary.channel_sessions_sha256.as_bytes(),
        summary.feed_specification_version.as_bytes(),
        summary.transport_specification_version.as_bytes(),
        summary.native_schema_version.as_bytes(),
        summary.serialization_version.as_bytes(),
        summary.decoder_implementation_version.as_bytes(),
        &summary.serialized_event_bytes.to_le_bytes(),
        &summary.decoded_event_batch_bytes.to_le_bytes(),
        summary.serialized_events_sha256.as_bytes(),
        &summary.first_capture_time_unix_nanos.to_le_bytes(),
        &summary.last_capture_time_unix_nanos.to_le_bytes(),
        &summary.first_send_time.value().to_le_bytes(),
        &summary.last_send_time.value().to_le_bytes(),
        &summary.first_source_time.value().to_le_bytes(),
        &summary.last_source_time.value().to_le_bytes(),
        &summary.source_clock_messages.to_le_bytes(),
        &summary.provider_timestamp_keys.to_le_bytes(),
        &summary.max_observed_send_capture_skew_nanos.to_le_bytes(),
        &[anomaly_policy_code(summary.anomaly_policy)],
    ])
}

fn decode_contract_identity(
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    limits: DecodeLimits,
    anomaly_policy: DecodeAnomalyPolicy,
    channel_contract: DecodeChannelContract,
    implementation_sha256: Sha256Digest,
) -> Sha256Digest {
    let channel_identity = match channel_contract {
        DecodeChannelContract::SingleActiveChannelOne => crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-single-active-channel-one/v1",
        ]),
        DecodeChannelContract::Dplc16(distribution) => distribution.contract_sha256(),
    };
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-decode-contract/v1",
        feed.catalog_name().as_bytes(),
        feed_version.catalog_value().as_bytes(),
        feed_version.specification_value().as_bytes(),
        transport_version.catalog_value().as_bytes(),
        transport_version.specification_value().as_bytes(),
        EVENT_SCHEMA_VERSION.as_bytes(),
        EVENT_SERIALIZATION_VERSION.as_bytes(),
        DECODER_IMPLEMENTATION_VERSION.as_bytes(),
        implementation_sha256.as_bytes(),
        limits.identity().as_bytes(),
        &[anomaly_policy_code(anomaly_policy)],
        channel_identity.as_bytes(),
    ])
}

fn decoder_implementation_fingerprint() -> Sha256Digest {
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-decoder-implementation/v3",
        b"classic-pcap-2.4:ethernet-vlan-ipv4-udp:iex-tp-1.26",
        b"TOPS-1.66:S10,D31,H22,I18,O18,P19,Q42,T38,X26,B38,A80",
        b"DEEP-1.08:S10,D31,H22,I18,O18,P19,E18,8:30,5:30,T38,X26,B38,A80",
        b"DEEP+-1.04:S10,D31,H22,I18,O18,P19,E18,a38,M38,R26,L46,T38,B38,C18",
        b"unknown-types-and-appended-fields:raw-parent-and-complete-message-digest",
        b"continuity:channel-session-sequence-stream-offset-start-end",
        b"clocks:capture-global,send-channel,source-family-symbol,source-le-send,send-capture-skew",
        b"serialization:ordinal-u64le,length-u64le,json-event-bytes",
    ])
}

fn dplc_distribution_identity(
    trade_date: TradeDate,
    roles: [DecodeChannelRole; MAX_DEEP_PLUS_CHANNELS],
    provider_evidence_sha256: Sha256Digest,
) -> Sha256Digest {
    let mut role_bytes = [0_u8; MAX_DEEP_PLUS_CHANNELS];
    for (index, role) in roles.into_iter().enumerate() {
        role_bytes[index] = match role {
            DecodeChannelRole::Active => 1,
            DecodeChannelRole::ReservedHeartbeatOnly => 2,
        };
    }
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-dplc-channel-distribution/v1",
        trade_date.compact().as_bytes(),
        &role_bytes,
        provider_evidence_sha256.as_bytes(),
    ])
}

const fn anomaly_policy_code(policy: DecodeAnomalyPolicy) -> u8 {
    match policy {
        DecodeAnomalyPolicy::RejectStructuralClockAndFamilyTimestampAnomaliesRetainExtensionsV2 => 2,
    }
}

fn nonzero_digest(digest: Sha256Digest) -> bool {
    digest.as_bytes().iter().any(|byte| *byte != 0)
}

/// PCAP, transport, continuity, or selected-message decode failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    /// Immutable plan-selected decoder contract is invalid or does not match the selection.
    #[error("IEX HIST immutable decoder contract is invalid")]
    InvalidDecoderContract,
    /// Authority-owned decode attempt does not match the immutable plan or decoder contract.
    #[error("IEX HIST decode attempt evidence is invalid")]
    InvalidDecodeAttempt,
    /// Selected feed/transport version is not implemented by this exact decoder.
    #[error("IEX HIST decoder version is unsupported")]
    UnsupportedVersion,
    /// Capture receipt did not descend from the selected plan.
    #[error("IEX HIST capture receipt does not match its plan")]
    ReceiptMismatch,
    /// Raw bytes are complete but their trusted chronology was already quarantined.
    #[error("IEX HIST capture chronology is quarantined")]
    CaptureChronologyQuarantined,
    /// Restored terminal accounting did not match its capture or content identity.
    #[error("IEX HIST terminal decode receipt identity is invalid")]
    SummaryIdentityMismatch,
    /// Decoder bounds were absent or invalid.
    #[error("IEX HIST decoder limits are invalid")]
    InvalidLimits,
    /// Fallible buffer/output reservation failed.
    #[error("IEX HIST decoder capacity is unavailable")]
    Capacity,
    /// Decoder was used after a terminal failure.
    #[error("IEX HIST decoder is poisoned")]
    Poisoned,
    /// One caller chunk exceeded the streaming boundary.
    #[error("IEX HIST PCAP input chunk is too large")]
    ChunkTooLarge,
    /// Exact PCAP bytes differed from the materialization receipt.
    #[error("IEX HIST PCAP length does not match its receipt")]
    PcapLengthMismatch,
    /// Exact PCAP digest differed from the materialization receipt.
    #[error("IEX HIST PCAP checksum does not match its receipt")]
    PcapChecksumMismatch,
    /// File ended inside a PCAP header or record.
    #[error("IEX HIST PCAP is truncated")]
    TruncatedPcap,
    /// Classic-PCAP version/header fields were malformed.
    #[error("IEX HIST PCAP header is malformed")]
    InvalidPcapHeader,
    /// PCAP format/link type is outside this decoder contract.
    #[error("IEX HIST PCAP format or link type is unsupported")]
    UnsupportedPcap,
    /// Packet exceeded the admitted PCAP/snapshot ceiling.
    #[error("IEX HIST captured packet exceeds its bound")]
    PacketTooLarge,
    /// Packet bytes were shorter than the capture metadata claimed.
    #[error("IEX HIST captured packet is truncated")]
    TruncatedPacket,
    /// File packet count exceeded its bound.
    #[error("IEX HIST PCAP packet count exceeds its bound")]
    PacketLimit,
    /// PCAP record timestamp was malformed.
    #[error("IEX HIST PCAP capture timestamp is invalid")]
    InvalidCaptureTimestamp,
    /// PCAP record timestamps regressed.
    #[error("IEX HIST PCAP capture clock regressed")]
    CaptureClockRegression,
    /// Absolute IEX-TP send versus PCAP-capture skew exceeded the plan-bound ceiling.
    #[error(
        "IEX HIST send/capture skew exceeded its bound on channel {channel_id}: observed {observed_skew_nanos}, admitted {admitted_skew_nanos}"
    )]
    SendCaptureClockSkew {
        /// Exact IEX-TP channel carrying the anomalous segment.
        channel_id: u32,
        /// Exact IEX-TP send clock.
        send_time_unix_nanos: u64,
        /// Exact PCAP capture clock.
        capture_time_unix_nanos: u64,
        /// Absolute observed skew.
        observed_skew_nanos: u64,
        /// Plan-bound maximum absolute skew.
        admitted_skew_nanos: u64,
    },
    /// Ethernet/IP layout is outside the closed boundary.
    #[error("IEX HIST captured packet layout is unsupported")]
    UnsupportedPacket,
    /// IPv4 header checksum failed.
    #[error("IEX HIST IPv4 checksum is invalid")]
    InvalidIpv4Checksum,
    /// UDP length disagreed with the containing IP packet.
    #[error("IEX HIST UDP length is malformed")]
    MalformedUdpLength,
    /// Nonzero UDP checksum failed.
    #[error("IEX HIST UDP checksum is invalid")]
    InvalidUdpChecksum,
    /// IEX-TP header was truncated.
    #[error("IEX HIST IEX-TP header is truncated")]
    TruncatedTransport,
    /// IEX-TP protocol/channel did not match the selected feed.
    #[error("IEX HIST IEX-TP protocol or channel does not match the selected feed")]
    WrongFeedOrChannel,
    /// A required DPLC channel was absent from the complete capture.
    #[error("IEX HIST required DPLC channel {channel_id} is absent")]
    MissingRequiredChannel {
        /// Exact missing channel identifier.
        channel_id: u32,
    },
    /// A date-effective reserved DPLC channel carried non-heartbeat payload.
    #[error("IEX HIST reserved DPLC channel {channel_id} carried payload")]
    ReservedChannelPayload {
        /// Exact reserved channel identifier.
        channel_id: u32,
    },
    /// Payload length/count fields were inconsistent.
    #[error("IEX HIST IEX-TP payload length or count is malformed")]
    MalformedTransportLength,
    /// Sequence/offset coordinate was negative or zero where prohibited.
    #[error("IEX HIST IEX-TP continuity coordinate is invalid")]
    InvalidContinuityCoordinate,
    /// Full-day capture did not begin at sequence 1 and stream offset 0.
    #[error("IEX HIST capture starts in the middle of a session")]
    CaptureStartsMidSession,
    /// Session identifier reset inside one selected file.
    #[error("IEX HIST IEX-TP session reset inside the capture")]
    SessionReset,
    /// Transport send timestamps regressed.
    #[error("IEX HIST IEX-TP send time regressed")]
    SendClockRegression,
    /// Exact higher-layer sequence gap.
    #[error("IEX HIST sequence gap: expected {expected}, received {actual}")]
    SequenceGap {
        /// Next required sequence.
        expected: i64,
        /// Received sequence.
        actual: i64,
    },
    /// Duplicate or out-of-order sequence.
    #[error("IEX HIST sequence duplicated or moved backward")]
    DuplicateOrOutOfOrderSequence,
    /// Exact byte-stream offset gap.
    #[error("IEX HIST stream-offset gap: expected {expected}, received {actual}")]
    StreamOffsetGap {
        /// Next required byte offset.
        expected: i64,
        /// Received byte offset.
        actual: i64,
    },
    /// Duplicate or out-of-order byte-stream offset.
    #[error("IEX HIST stream offset duplicated or moved backward")]
    DuplicateOrOutOfOrderOffset,
    /// Sequence arithmetic overflowed.
    #[error("IEX HIST sequence arithmetic overflowed")]
    SequenceOverflow,
    /// Stream-offset arithmetic overflowed.
    #[error("IEX HIST stream-offset arithmetic overflowed")]
    StreamOffsetOverflow,
    /// Higher-layer message length was zero, excessive, or overflowed.
    #[error("IEX HIST message length is malformed")]
    MalformedMessageLength,
    /// Higher-layer message body was shorter than its framed length or selected prefix.
    #[error("IEX HIST message is truncated")]
    TruncatedMessage,
    /// Message count exceeded the configured session/segment limit.
    #[error("IEX HIST message count exceeds its bound")]
    MessageLimit,
    /// Exact framed native decoded-event bytes exceeded the plan reservation.
    #[error("IEX HIST decoded event batch exceeds its admitted byte ceiling")]
    DecodedEventBatchBytesExceeded,
    /// Exact timestamp was negative or outside the selected feed-date window.
    #[error("IEX HIST source timestamp is invalid")]
    InvalidTimestamp,
    /// Source event timestamp exceeded its segment send time.
    #[error("IEX HIST event timestamp occurs after its segment send time")]
    EventAfterSendTime,
    /// Distinct provider timestamp coordinates exceeded the plan-bound state ceiling.
    #[error("IEX HIST provider timestamp state exceeds its admitted key ceiling")]
    ProviderTimestampStateLimit,
    /// A provider source clock regressed for one exact message-type/symbol coordinate.
    #[error("IEX HIST provider timestamp regressed for message type {message_type}")]
    ProviderTimestampRegression {
        /// Exact provider message-type byte.
        message_type: u8,
        /// Exact fixed-width provider symbol, zero-filled only for an absent symbol.
        symbol: [u8; 8],
        /// Previously retained source clock for this exact coordinate.
        previous_source_time_unix_nanos: i64,
        /// Regressing source clock.
        actual_source_time_unix_nanos: i64,
    },
    /// Fixed-point price or paired size was impossible.
    #[error("IEX HIST exact price or size is invalid")]
    InvalidPriceOrSize,
    /// Fixed-width ASCII field was malformed.
    #[error("IEX HIST fixed-width text is invalid")]
    InvalidText,
    /// Enumerated provider message value was invalid.
    #[error("IEX HIST message value is invalid")]
    InvalidMessageValue,
    /// Required start/end system-message order was invalid.
    #[error("IEX HIST session markers are invalid")]
    InvalidSessionMarkers,
    /// A message followed the terminal end-of-messages marker.
    #[error("IEX HIST message appears after end of messages")]
    MessageAfterSessionEnd,
    /// File did not contain a complete start-to-end non-heartbeat session.
    #[error("IEX HIST capture does not contain a complete feed session")]
    IncompleteSession,
    /// Downstream event retention refused a validated event.
    #[error("IEX HIST downstream event sink rejected publication")]
    SinkRejected,
    /// Canonical native-event serialization failed before staging.
    #[error("IEX HIST native event serialization failed")]
    Serialization,
    /// Transactional sink did not return the decoder-issued terminal commit identity.
    #[error("IEX HIST downstream event sink commit receipt is invalid")]
    SinkCommitMismatch,
}
