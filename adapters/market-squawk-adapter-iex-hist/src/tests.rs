#![allow(
    clippy::panic,
    clippy::unwrap_used,
    reason = "the three closed synthetic proofs terminate immediately when fixture construction fails"
)]

use crc32fast::Hasher as Crc32;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::{
    ByteAdmissionLimits, CaptureResponseMetadata, Catalog, CatalogTransportMetadata, ColdJobPlan,
    ColdJobTrigger, DecodeError, DecodeLimits, ExactFileRequest, FeedKind, FeedVersion,
    GzipPcapReceiptBuilder, IexEvent, IexHistPlanner, PcapMaterializationReceipt,
    PcapStreamDecoder, PriceUnits1e4, ScheduleLane, TradeDate, TransportVersion,
};

const OBSERVED_ON: &str = "20260811";
const TRADE_DATE: &str = "20260810";
const FILE_NAME: &str = "20260810_IEXTP1_TOPS1.6.pcap.gz";
const DOWNLOAD_URL: &str = "https://www.googleapis.com/download/storage/v1/b/iex/o/data%2Ffeeds%2F20260810%2F20260810_IEXTP1_TOPS1.6.pcap.gz?generation=1786415919114081&alt=media";

#[test]
fn catalog_receipt_and_exact_cold_byte_plan_are_retained() {
    let body = catalog_body(1_000);
    let catalog = parse_catalog(&body);
    assert_eq!(
        catalog.receipt().body_sha256.to_hex(),
        "b0f5a442f75f575970e12c7b35b7e5d9b4c97c3ce7bd1fdc70d3e708e42c1314"
    );
    assert_eq!(catalog.receipt().date_count, 1);
    assert_eq!(catalog.receipt().file_count, 1);
    assert_eq!(catalog.receipt().advertised_compressed_bytes, 1_000);

    let selected = select_tops(&catalog);
    let plan = plan(selected, 4_000, 10_000);
    assert_eq!(plan.lane(), ScheduleLane::Cold);
    assert!(!plan.automatic_archive_catch_up());
    assert_eq!(plan.max_parallel_transfers(), 1);
    assert_eq!(plan.earliest_available_on().compact(), "20260811");
    assert_eq!(plan.rolling_window_start().compact(), "20250811");
    assert_eq!(plan.required_disk_bytes(), 6_000);
}

#[test]
fn synthetic_tops_sequence_maps_exactly_and_refuses_gap_and_corruption() {
    let valid_pcap = build_valid_pcap();
    let valid_gzip = stored_gzip(&valid_pcap);
    let plan = fixture_plan(u64::try_from(valid_gzip.len()).unwrap_or(u64::MAX), 8_192);
    let receipt = capture_receipt(&plan, &valid_gzip, &valid_pcap);
    let mut decoder = PcapStreamDecoder::new(
        &plan,
        &receipt,
        DecodeLimits {
            max_stream_chunk_bytes: 37,
            max_packet_bytes: 2_048,
            max_packets: 8,
            max_messages: 8,
        },
    )
    .unwrap_or_else(|error| panic!("decoder setup failed: {error}"));
    let mut events = Vec::new();
    for chunk in valid_pcap.chunks(37) {
        decoder
            .push(chunk, &mut events)
            .unwrap_or_else(|error| panic!("valid PCAP failed: {error}"));
    }
    let summary = decoder
        .finish()
        .unwrap_or_else(|error| panic!("valid PCAP did not finish: {error}"));
    assert_eq!(summary.messages, 3);
    assert_eq!(summary.next_sequence, 4);
    assert_eq!(events.len(), 3);
    assert_eq!(events[1].sequence, 2);
    assert_eq!(events[1].stream_offset, 12);
    assert_eq!(events[1].message_data_bytes, 42);
    match &events[1].event {
        IexEvent::Quote {
            symbol,
            bid_size,
            bid_price,
            ask_price,
            ask_size,
            ..
        } => {
            assert_eq!(symbol, "AAPL");
            assert_eq!(*bid_size, 100);
            assert_eq!(*bid_price, PriceUnits1e4::try_new(1_900_000).unwrap());
            assert_eq!(*ask_price, PriceUnits1e4::try_new(1_900_100).unwrap());
            assert_eq!(*ask_size, 120);
        }
        other => panic!("expected exact TOPS quote, got {other:?}"),
    }

    let mut gapped = valid_pcap.clone();
    rewrite_second_sequence(&mut gapped, 4);
    let gapped_gzip = stored_gzip(&gapped);
    let gapped_receipt = capture_receipt(&plan, &gapped_gzip, &gapped);
    let mut decoder = PcapStreamDecoder::new(&plan, &gapped_receipt, DecodeLimits::default())
        .unwrap_or_else(|error| panic!("gap decoder setup failed: {error}"));
    assert!(matches!(
        decoder.push(&gapped, &mut Vec::new()),
        Err(DecodeError::SequenceGap {
            expected: 3,
            actual: 4
        })
    ));

    let mut corrupted = valid_pcap.clone();
    let quote_price_offset = first_packet_data_offset(&corrupted) + 40 + 2 + 10 + 2 + 22;
    corrupted[quote_price_offset] ^= 0x01;
    let corrupt_gzip = stored_gzip(&corrupted);
    let corrupt_receipt = capture_receipt(&plan, &corrupt_gzip, &corrupted);
    let mut decoder = PcapStreamDecoder::new(&plan, &corrupt_receipt, DecodeLimits::default())
        .unwrap_or_else(|error| panic!("corrupt decoder setup failed: {error}"));
    assert_eq!(
        decoder.push(&corrupted, &mut Vec::new()),
        Err(DecodeError::InvalidUdpChecksum)
    );
}

#[tokio::test]
async fn mock_selected_file_stream_is_staged_expanded_receipted_and_decoded() {
    let pcap = build_valid_pcap();
    let gzip = stored_gzip(&pcap);
    let mut plan = fixture_plan(u64::try_from(gzip.len()).unwrap_or(u64::MAX), 8_192);
    let now = current_unix_nanos();
    plan.deadline_unix_nanos = now + 30_000_000_000;
    let staging = tempfile::tempdir().unwrap();
    let mut events = Vec::new();
    let chunks = gzip.chunks(13).map(Bytes::copy_from_slice).collect();
    let outcome = crate::transport::materialize_mock_stream(
        &plan,
        CaptureResponseMetadata {
            response_url: DOWNLOAD_URL.to_owned(),
            status: 200,
            content_length: u64::try_from(gzip.len()).unwrap_or(u64::MAX),
            content_encoding: Some("identity".to_owned()),
            etag: Some("mock-stream".to_owned()),
            response_started_at_unix_nanos: now,
        },
        chunks,
        staging.path(),
        &CancellationToken::new(),
        &mut events,
    )
    .await
    .unwrap_or_else(|error| panic!("mock selected-file stream failed: {error}"));

    assert_eq!(outcome.telemetry().attempts_total(), 1);
    assert_eq!(
        outcome.telemetry().response_bytes(),
        u64::try_from(gzip.len()).unwrap_or(u64::MAX)
    );
    assert_eq!(
        outcome.telemetry().expanded_pcap_bytes(),
        u64::try_from(pcap.len()).unwrap_or(u64::MAX)
    );
    assert_eq!(outcome.decode().messages, 3);
    assert_eq!(events.len(), 3);

    let (_, _, _, files) = outcome.into_parts();
    let mut retained_gzip = Vec::new();
    files
        .reopen_compressed()
        .unwrap()
        .read_to_end(&mut retained_gzip)
        .unwrap();
    let mut retained_pcap = Vec::new();
    files
        .reopen_pcap()
        .unwrap()
        .read_to_end(&mut retained_pcap)
        .unwrap();
    assert_eq!(retained_gzip, gzip);
    assert_eq!(retained_pcap, pcap);
}

fn current_unix_nanos() -> i64 {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    i64::try_from(duration.as_nanos()).unwrap()
}

fn parse_catalog(body: &[u8]) -> Catalog {
    Catalog::parse(
        body,
        CatalogTransportMetadata {
            status: 200,
            content_type: "application/json;charset=utf-8".to_owned(),
            content_length: u64::try_from(body.len()).unwrap_or(u64::MAX),
            etag: Some("W/\"fixture\"".to_owned()),
            retrieved_at_unix_nanos: 1_786_425_600_000_000_000,
            observed_on: TradeDate::parse(OBSERVED_ON).unwrap(),
        },
    )
    .unwrap_or_else(|error| panic!("catalog fixture failed: {error}"))
}

fn select_tops(catalog: &Catalog) -> crate::SelectedFileReceipt {
    catalog
        .select(&ExactFileRequest {
            trade_date: TradeDate::parse(TRADE_DATE).unwrap(),
            feed: FeedKind::Tops,
            feed_version: FeedVersion::Tops1_6,
            transport_version: TransportVersion::IexTp1,
            file_name: FILE_NAME.to_owned(),
        })
        .unwrap_or_else(|error| panic!("catalog selection failed: {error}"))
}

fn plan(
    selected: crate::SelectedFileReceipt,
    max_pcap_bytes: u64,
    available_disk_bytes: u64,
) -> ColdJobPlan {
    IexHistPlanner::plan(
        selected,
        ColdJobTrigger::ResearchJob,
        ByteAdmissionLimits {
            max_compressed_bytes: 2_000,
            max_pcap_bytes,
            available_disk_bytes,
            required_free_reserve_bytes: 1_000,
            now_unix_nanos: 1_786_425_600_000_000_000,
            deadline_unix_nanos: 1_786_429_200_000_000_000,
        },
    )
    .unwrap_or_else(|error| panic!("cold plan failed: {error}"))
}

fn fixture_plan(compressed_bytes: u64, max_pcap_bytes: u64) -> ColdJobPlan {
    let body = catalog_body(compressed_bytes);
    let catalog = parse_catalog(&body);
    plan(select_tops(&catalog), max_pcap_bytes, 32_768)
}

fn catalog_body(size: u64) -> Vec<u8> {
    format!(
        "{{\"{TRADE_DATE}\":[{{\"link\":\"{DOWNLOAD_URL}\",\"date\":\"{TRADE_DATE}\",\"feed\":\"TOPS\",\"version\":\"1.6\",\"protocol\":\"IEXTP1\",\"size\":\"{size}\"}}]}}"
    )
    .into_bytes()
}

fn capture_receipt(plan: &ColdJobPlan, gzip: &[u8], pcap: &[u8]) -> PcapMaterializationReceipt {
    let mut builder = GzipPcapReceiptBuilder::new(
        plan,
        CaptureResponseMetadata {
            response_url: DOWNLOAD_URL.to_owned(),
            status: 200,
            content_length: u64::try_from(gzip.len()).unwrap_or(u64::MAX),
            content_encoding: Some("identity".to_owned()),
            etag: Some("fixture-object".to_owned()),
            response_started_at_unix_nanos: 1_786_425_600_000_000_001,
        },
    )
    .unwrap_or_else(|error| panic!("capture setup failed: {error}"));
    for chunk in gzip.chunks(17) {
        builder
            .push_compressed(chunk)
            .unwrap_or_else(|error| panic!("compressed fixture failed: {error}"));
    }
    for chunk in pcap.chunks(19) {
        builder
            .push_pcap(chunk)
            .unwrap_or_else(|error| panic!("PCAP fixture failed: {error}"));
    }
    builder
        .finish(1_786_425_600_000_000_002)
        .unwrap_or_else(|error| panic!("capture receipt failed: {error}"))
}

fn build_valid_pcap() -> Vec<u8> {
    let date = TradeDate::parse(TRADE_DATE).unwrap();
    let base = date.start_epoch_nanos().unwrap() + 50_000_000_000_000;
    let start = system_message(b'O', base);
    let quote = quote_message(base + 1_000);
    let end = system_message(b'C', base + 2_000);
    let first = iex_segment(&[start, quote], 0, 1, base + 1_500);
    let second_offset = i64::try_from(first.len() - 40).unwrap_or(i64::MAX);
    let second = iex_segment(&[end], second_offset, 3, base + 2_500);

    let mut pcap = Vec::new();
    pcap.extend_from_slice(&[0x4d, 0x3c, 0xb2, 0xa1]);
    pcap.extend_from_slice(&2_u16.to_le_bytes());
    pcap.extend_from_slice(&4_u16.to_le_bytes());
    pcap.extend_from_slice(&0_i32.to_le_bytes());
    pcap.extend_from_slice(&0_u32.to_le_bytes());
    pcap.extend_from_slice(&65_535_u32.to_le_bytes());
    pcap.extend_from_slice(&1_u32.to_le_bytes());
    append_record(&mut pcap, &ethernet_udp(&first), base + 10_000);
    append_record(&mut pcap, &ethernet_udp(&second), base + 20_000);
    pcap
}

fn system_message(code: u8, timestamp: i64) -> Vec<u8> {
    let mut message = vec![b'S', code];
    message.extend_from_slice(&timestamp.to_le_bytes());
    message
}

fn quote_message(timestamp: i64) -> Vec<u8> {
    let mut message = vec![b'Q', 0];
    message.extend_from_slice(&timestamp.to_le_bytes());
    message.extend_from_slice(b"AAPL    ");
    message.extend_from_slice(&100_u32.to_le_bytes());
    message.extend_from_slice(&1_900_000_i64.to_le_bytes());
    message.extend_from_slice(&1_900_100_i64.to_le_bytes());
    message.extend_from_slice(&120_u32.to_le_bytes());
    message
}

fn iex_segment(
    messages: &[Vec<u8>],
    stream_offset: i64,
    first_sequence: i64,
    send_time: i64,
) -> Vec<u8> {
    let mut payload = Vec::new();
    for message in messages {
        payload.extend_from_slice(
            &u16::try_from(message.len())
                .unwrap_or(u16::MAX)
                .to_le_bytes(),
        );
        payload.extend_from_slice(message);
    }
    let mut segment = vec![1, 0];
    segment.extend_from_slice(&0x8003_u16.to_le_bytes());
    segment.extend_from_slice(&1_u32.to_le_bytes());
    segment.extend_from_slice(&7_u32.to_le_bytes());
    segment.extend_from_slice(
        &u16::try_from(payload.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    segment.extend_from_slice(
        &u16::try_from(messages.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    segment.extend_from_slice(&stream_offset.to_le_bytes());
    segment.extend_from_slice(&first_sequence.to_le_bytes());
    segment.extend_from_slice(&send_time.to_le_bytes());
    segment.extend_from_slice(&payload);
    segment
}

fn ethernet_udp(payload: &[u8]) -> Vec<u8> {
    let udp_length = u16::try_from(8 + payload.len()).unwrap_or(u16::MAX);
    let ip_length = u16::try_from(20 + usize::from(udp_length)).unwrap_or(u16::MAX);
    let mut frame = Vec::new();
    frame.extend_from_slice(&[1, 0, 94, 0, 0, 1]);
    frame.extend_from_slice(&[2, 0, 0, 0, 0, 1]);
    frame.extend_from_slice(&0x0800_u16.to_be_bytes());
    let ip_start = frame.len();
    frame.extend_from_slice(&[0x45, 0]);
    frame.extend_from_slice(&ip_length.to_be_bytes());
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.extend_from_slice(&0x4000_u16.to_be_bytes());
    frame.extend_from_slice(&[64, 17]);
    frame.extend_from_slice(&0_u16.to_be_bytes());
    frame.extend_from_slice(&[10, 0, 0, 1]);
    frame.extend_from_slice(&[233, 215, 21, 3]);
    let ip_checksum = checksum(&[&frame[ip_start..ip_start + 20]]);
    frame[ip_start + 10..ip_start + 12].copy_from_slice(&ip_checksum.to_be_bytes());
    let udp_start = frame.len();
    frame.extend_from_slice(&10_377_u16.to_be_bytes());
    frame.extend_from_slice(&10_377_u16.to_be_bytes());
    frame.extend_from_slice(&udp_length.to_be_bytes());
    frame.extend_from_slice(&0_u16.to_be_bytes());
    frame.extend_from_slice(payload);
    let pseudo = [
        10,
        0,
        0,
        1,
        233,
        215,
        21,
        3,
        0,
        17,
        udp_length.to_be_bytes()[0],
        udp_length.to_be_bytes()[1],
    ];
    let mut udp_checksum = checksum(&[&pseudo, &frame[udp_start..]]);
    if udp_checksum == 0 {
        udp_checksum = 0xffff;
    }
    frame[udp_start + 6..udp_start + 8].copy_from_slice(&udp_checksum.to_be_bytes());
    frame
}

fn append_record(pcap: &mut Vec<u8>, frame: &[u8], capture_nanos: i64) {
    let seconds = u32::try_from(capture_nanos / 1_000_000_000).unwrap_or(u32::MAX);
    let nanos = u32::try_from(capture_nanos % 1_000_000_000).unwrap_or(u32::MAX);
    let length = u32::try_from(frame.len()).unwrap_or(u32::MAX);
    pcap.extend_from_slice(&seconds.to_le_bytes());
    pcap.extend_from_slice(&nanos.to_le_bytes());
    pcap.extend_from_slice(&length.to_le_bytes());
    pcap.extend_from_slice(&length.to_le_bytes());
    pcap.extend_from_slice(frame);
}

fn stored_gzip(payload: &[u8]) -> Vec<u8> {
    let length = u16::try_from(payload.len()).unwrap_or(u16::MAX);
    let mut gzip = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255, 1];
    gzip.extend_from_slice(&length.to_le_bytes());
    gzip.extend_from_slice(&(!length).to_le_bytes());
    gzip.extend_from_slice(payload);
    let mut crc = Crc32::new();
    crc.update(payload);
    gzip.extend_from_slice(&crc.finalize().to_le_bytes());
    gzip.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    gzip
}

fn rewrite_second_sequence(pcap: &mut [u8], sequence: i64) {
    let first_length =
        usize::try_from(u32::from_le_bytes(pcap[32..36].try_into().unwrap())).unwrap_or(usize::MAX);
    let second_record = 24 + 16 + first_length;
    let second_packet = second_record + 16;
    let iex = second_packet + 14 + 20 + 8;
    pcap[iex + 24..iex + 32].copy_from_slice(&sequence.to_le_bytes());
    rewrite_udp_checksum(&mut pcap[second_packet..]);
}

fn first_packet_data_offset(_: &[u8]) -> usize {
    24 + 16 + 14 + 20 + 8
}

fn rewrite_udp_checksum(frame: &mut [u8]) {
    let udp_start = 14 + 20;
    frame[udp_start + 6..udp_start + 8].fill(0);
    let udp_length = [frame[udp_start + 4], frame[udp_start + 5]];
    let pseudo = [
        frame[26],
        frame[27],
        frame[28],
        frame[29],
        frame[30],
        frame[31],
        frame[32],
        frame[33],
        0,
        17,
        udp_length[0],
        udp_length[1],
    ];
    let udp_len = usize::from(u16::from_be_bytes(udp_length));
    let mut value = checksum(&[&pseudo, &frame[udp_start..udp_start + udp_len]]);
    if value == 0 {
        value = 0xffff;
    }
    frame[udp_start + 6..udp_start + 8].copy_from_slice(&value.to_be_bytes());
}

fn checksum(parts: &[&[u8]]) -> u16 {
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
    !u16::try_from(sum).unwrap_or(0)
}
