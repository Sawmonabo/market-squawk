use std::collections::TryReserveError;

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::model::{
    DecodedIexEvent, EpochNanos, FeedKind, FeedVersion, IexEvent, IexVenueSemantics, ModelError,
    PriceLevelSide, PriceType, PriceUnits1e4, Sha256Digest, SystemEventCode, TradeDate,
    TradingStatus, TransportVersion,
};
use crate::planning::ColdJobPlan;
use crate::receipt::PcapMaterializationReceipt;

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

/// Resource limits for one streaming PCAP decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeLimits {
    /// Largest caller chunk accepted by `push`.
    pub max_stream_chunk_bytes: usize,
    /// Largest captured Ethernet frame accepted.
    pub max_packet_bytes: u32,
    /// Maximum PCAP records admitted for the file.
    pub max_packets: u64,
    /// Maximum higher-layer messages admitted for the file.
    pub max_messages: u64,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_stream_chunk_bytes: 128 * 1024,
            max_packet_bytes: 65_535,
            max_packets: 200_000_000,
            max_messages: 1_000_000_000,
        }
    }
}

/// Terminal accounting for one completely validated PCAP.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DecodeSummary {
    /// Parent capture receipt.
    pub capture_receipt_sha256: Sha256Digest,
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
    /// First IEX-TP session identifier.
    pub session_id: u32,
    /// Next sequence expected after the complete end-of-messages message.
    pub next_sequence: i64,
    /// Next stream byte offset expected after the complete session.
    pub next_stream_offset: i64,
}

/// Bounded event consumer used by the streaming decoder.
pub trait IexEventSink {
    /// Accepts one fully validated event.
    ///
    /// # Errors
    ///
    /// Returning an error poisons the decoder so a partial publication cannot be resumed as if
    /// it were complete.
    fn emit(&mut self, event: DecodedIexEvent) -> Result<(), SinkError>;
}

impl IexEventSink for Vec<DecodedIexEvent> {
    fn emit(&mut self, event: DecodedIexEvent) -> Result<(), SinkError> {
        self.try_reserve(1).map_err(|_| SinkError::Rejected)?;
        self.push(event);
        Ok(())
    }
}

/// Sink refusal at the publication boundary.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SinkError {
    /// The downstream boundary could not retain the event.
    #[error("IEX HIST event sink rejected an event")]
    Rejected,
}

/// Incremental classic-PCAP and IEX-TP decoder bound to one exact selected file and receipt.
#[derive(Debug)]
pub struct PcapStreamDecoder {
    trade_date: TradeDate,
    feed: FeedKind,
    feed_version: FeedVersion,
    transport_version: TransportVersion,
    source_file_identity: Sha256Digest,
    expected_pcap_sha256: Sha256Digest,
    expected_pcap_bytes: u64,
    capture_receipt_sha256: Sha256Digest,
    limits: DecodeLimits,
    pcap_hasher: Sha256,
    bytes_seen: u64,
    buffer: Vec<u8>,
    pcap_format: Option<PcapFormat>,
    continuity: Option<Continuity>,
    previous_capture_time: Option<u64>,
    packets: u64,
    segments: u64,
    messages: u64,
    unmapped_messages: u64,
    saw_start_messages: bool,
    saw_end_messages: bool,
    poisoned: bool,
}

impl PcapStreamDecoder {
    /// Creates a decoder only when plan, selected descriptor, gzip receipt, and PCAP bounds agree.
    ///
    /// # Errors
    ///
    /// Rejects unsupported feed versions and any parent/digest/size mismatch before bytes decode.
    pub fn new(
        plan: &ColdJobPlan,
        receipt: &PcapMaterializationReceipt,
        limits: DecodeLimits,
    ) -> Result<Self, DecodeError> {
        if plan.selected_file.feed_version.feed() != plan.selected_file.feed
            || !matches!(
                (plan.selected_file.feed, plan.selected_file.feed_version),
                (FeedKind::Tops, FeedVersion::Tops1_6) | (FeedKind::Deep, FeedVersion::Deep1_0)
            )
        {
            return Err(DecodeError::UnsupportedVersion);
        }
        if plan.selected_file.transport_version != TransportVersion::IexTp1 {
            return Err(DecodeError::UnsupportedVersion);
        }
        if receipt.plan_sha256 != plan.plan_sha256
            || receipt.selected_file_identity != plan.selected_file.identity()
            || receipt.compressed_bytes != plan.advertised_compressed_bytes
            || receipt.pcap_bytes > plan.max_pcap_bytes
        {
            return Err(DecodeError::ReceiptMismatch);
        }
        if limits.max_stream_chunk_bytes == 0
            || limits.max_stream_chunk_bytes > MAX_CALLER_CHUNK_BYTES
            || limits.max_packet_bytes < 64
            || limits.max_packet_bytes > 65_535
            || limits.max_packets == 0
            || limits.max_messages == 0
        {
            return Err(DecodeError::InvalidLimits);
        }
        let initial_capacity = limits
            .max_stream_chunk_bytes
            .min(usize::try_from(limits.max_packet_bytes).unwrap_or(usize::MAX))
            .max(PCAP_GLOBAL_HEADER_BYTES);
        let mut buffer = Vec::new();
        buffer
            .try_reserve(initial_capacity)
            .map_err(|_| DecodeError::Capacity)?;
        Ok(Self {
            trade_date: plan.selected_file.trade_date,
            feed: plan.selected_file.feed,
            feed_version: plan.selected_file.feed_version,
            transport_version: plan.selected_file.transport_version,
            source_file_identity: receipt.receipt_sha256,
            expected_pcap_sha256: receipt.pcap_sha256,
            expected_pcap_bytes: receipt.pcap_bytes,
            capture_receipt_sha256: receipt.receipt_sha256,
            limits,
            pcap_hasher: Sha256::new(),
            bytes_seen: 0,
            buffer,
            pcap_format: None,
            continuity: None,
            previous_capture_time: None,
            packets: 0,
            segments: 0,
            messages: 0,
            unmapped_messages: 0,
            saw_start_messages: false,
            saw_end_messages: false,
            poisoned: false,
        })
    }

    /// Pushes one bounded PCAP byte chunk and emits only completely validated messages.
    ///
    /// # Errors
    ///
    /// Any framing, checksum, continuity, message, resource, or sink failure poisons the session.
    pub fn push(&mut self, bytes: &[u8], sink: &mut dyn IexEventSink) -> Result<(), DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        let result = self.push_inner(bytes, sink);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Finalizes only a complete, checksum-identical, start-to-end feed session.
    ///
    /// # Errors
    ///
    /// Rejects prior failure, truncation, checksum mismatch, empty captures, or a missing session
    /// start/end marker.
    pub fn finish(mut self) -> Result<DecodeSummary, DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        if !self.buffer.is_empty() || self.pcap_format.is_none() {
            return Err(DecodeError::TruncatedPcap);
        }
        if self.bytes_seen != self.expected_pcap_bytes {
            return Err(DecodeError::PcapLengthMismatch);
        }
        let actual = Sha256Digest::from_bytes(self.pcap_hasher.finalize().into());
        if actual != self.expected_pcap_sha256 {
            return Err(DecodeError::PcapChecksumMismatch);
        }
        if self.packets == 0
            || self.segments == 0
            || !self.saw_start_messages
            || !self.saw_end_messages
        {
            return Err(DecodeError::IncompleteSession);
        }
        let continuity = self
            .continuity
            .take()
            .ok_or(DecodeError::IncompleteSession)?;
        Ok(DecodeSummary {
            capture_receipt_sha256: self.capture_receipt_sha256,
            pcap_bytes: self.bytes_seen,
            packets: self.packets,
            segments: self.segments,
            messages: self.messages,
            unmapped_messages: self.unmapped_messages,
            session_id: continuity.session_id,
            next_sequence: continuity.next_sequence,
            next_stream_offset: continuity.next_stream_offset,
        })
    }

    fn push_inner(&mut self, bytes: &[u8], sink: &mut dyn IexEventSink) -> Result<(), DecodeError> {
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
        self.process_buffer(sink)
    }

    fn process_buffer(&mut self, sink: &mut dyn IexEventSink) -> Result<(), DecodeError> {
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
            let packet = &self.buffer[PCAP_RECORD_HEADER_BYTES..record_bytes];
            decode_packet(
                PacketContext {
                    trade_date: self.trade_date,
                    feed: self.feed,
                    feed_version: self.feed_version,
                    transport_version: self.transport_version,
                    source_file_identity: self.source_file_identity,
                    capture_time_unix_nanos: record.capture_time_unix_nanos,
                    message_limit: self.limits.max_messages,
                },
                packet,
                &mut self.continuity,
                &mut self.saw_start_messages,
                &mut self.saw_end_messages,
                &mut self.messages,
                &mut self.unmapped_messages,
                sink,
            )?;
            self.previous_capture_time = Some(record.capture_time_unix_nanos);
            self.packets = self
                .packets
                .checked_add(1)
                .ok_or(DecodeError::PacketLimit)?;
            self.segments = self
                .segments
                .checked_add(1)
                .ok_or(DecodeError::PacketLimit)?;
            self.buffer.drain(..record_bytes);
        }
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
    capture_time_unix_nanos: u64,
    message_limit: u64,
}

#[derive(Clone, Copy, Debug)]
struct Continuity {
    session_id: u32,
    next_sequence: i64,
    next_stream_offset: i64,
    last_send_time: EpochNanos,
}

#[allow(
    clippy::too_many_arguments,
    reason = "packet decode updates one cohesive continuity/accounting boundary"
)]
fn decode_packet(
    context: PacketContext,
    packet: &[u8],
    continuity: &mut Option<Continuity>,
    saw_start_messages: &mut bool,
    saw_end_messages: &mut bool,
    messages: &mut u64,
    unmapped_messages: &mut u64,
    sink: &mut dyn IexEventSink,
) -> Result<(), DecodeError> {
    let udp_payload = extract_udp_payload(packet)?;
    if udp_payload.len() < IEX_TP_HEADER_BYTES {
        return Err(DecodeError::TruncatedTransport);
    }
    let header = parse_transport_header(&udp_payload[..IEX_TP_HEADER_BYTES], context)?;
    let payload = &udp_payload[IEX_TP_HEADER_BYTES..];
    if payload.len() != usize::from(header.payload_length) {
        return Err(DecodeError::MalformedTransportLength);
    }
    let heartbeat = header.payload_length == 0 && header.message_count == 0;
    if (header.payload_length == 0) != (header.message_count == 0) {
        return Err(DecodeError::MalformedTransportLength);
    }

    validate_continuity(*continuity, header, heartbeat)?;
    if heartbeat {
        if continuity.is_none() {
            *continuity = Some(Continuity {
                session_id: header.session_id,
                next_sequence: header.first_sequence,
                next_stream_offset: header.stream_offset,
                last_send_time: header.send_time,
            });
        } else if let Some(state) = continuity.as_mut() {
            state.last_send_time = header.send_time;
        }
        return Ok(());
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
    let mut local_start = *saw_start_messages;
    let mut local_end = *saw_end_messages;
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
    for event in decoded {
        sink.emit(event).map_err(|_| DecodeError::SinkRejected)?;
    }
    let next_sequence = header
        .first_sequence
        .checked_add(i64::from(header.message_count))
        .ok_or(DecodeError::SequenceOverflow)?;
    let next_stream_offset = header
        .stream_offset
        .checked_add(i64::from(header.payload_length))
        .ok_or(DecodeError::StreamOffsetOverflow)?;
    *continuity = Some(Continuity {
        session_id: header.session_id,
        next_sequence,
        next_stream_offset,
        last_send_time: header.send_time,
    });
    *saw_start_messages = local_start;
    *saw_end_messages = local_end;
    *messages = next_total;
    *unmapped_messages = unmapped_messages
        .checked_add(local_unmapped)
        .ok_or(DecodeError::MessageLimit)?;
    Ok(())
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
    if protocol_id != context.feed.protocol_id() || channel_id != 1 || session_id == 0 {
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
        (FeedKind::Deep, b'E') => parse_security_event(bytes, trade_date)?,
        (_, b'H') => parse_trading_status(bytes, trade_date)?,
        (FeedKind::Tops, b'Q') => parse_quote(bytes, trade_date)?,
        (FeedKind::Deep, b'8') => parse_price_level(bytes, trade_date, PriceLevelSide::Buy)?,
        (FeedKind::Deep, b'5') => parse_price_level(bytes, trade_date, PriceLevelSide::Sell)?,
        (_, b'T') => parse_trade(bytes, trade_date, false)?,
        (_, b'B') => parse_trade(bytes, trade_date, true)?,
        (_, b'X') => parse_official_price(bytes, trade_date)?,
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

fn parse_security_event(bytes: &[u8], trade_date: TradeDate) -> Result<ParsedMessage, DecodeError> {
    require_prefix(bytes, 18)?;
    let event = match bytes[1] {
        b'O' | b'C' => char::from(bytes[1]),
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
        | IexEvent::SecurityEvent { source_time, .. }
        | IexEvent::TradingStatus { source_time, .. }
        | IexEvent::Quote { source_time, .. }
        | IexEvent::PriceLevel { source_time, .. }
        | IexEvent::Trade { source_time, .. }
        | IexEvent::TradeBreak { source_time, .. }
        | IexEvent::OfficialPrice { source_time, .. } => Some(*source_time),
        IexEvent::Unmapped { .. } => None,
    }
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

/// PCAP, transport, continuity, or selected-message decode failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    /// Selected feed/transport version is not implemented by this exact decoder.
    #[error("IEX HIST decoder version is unsupported")]
    UnsupportedVersion,
    /// Capture receipt did not descend from the selected plan.
    #[error("IEX HIST capture receipt does not match its plan")]
    ReceiptMismatch,
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
    /// Exact timestamp was negative or outside the selected feed-date window.
    #[error("IEX HIST source timestamp is invalid")]
    InvalidTimestamp,
    /// Source event timestamp exceeded its segment send time.
    #[error("IEX HIST event timestamp occurs after its segment send time")]
    EventAfterSendTime,
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
}
