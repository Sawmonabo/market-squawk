#![allow(
    clippy::panic,
    clippy::unwrap_used,
    reason = "the one closed production-shaped proof terminates immediately on fixture failure"
)]

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use crc32fast::Hasher as Crc32;
use tokio_util::sync::CancellationToken;

use crate::catalog::CatalogTransportMetadata;
use crate::receipt::CaptureResponseMetadata;
use crate::transport::{MockStreamChunk, materialize_mock_stream, resume_mock_stream};
use crate::{
    ByteAdmissionLimits, CaptureChronologyDisposition, CaptureError, Catalog, ColdJobPlan,
    ColdJobTrigger, DecodeLimits, ExactFileRequest, FeedKind, FeedVersion, IexEvent,
    IexHistAuthorityClockSample, IexHistBarInterval, IexHistCapacityAuthority,
    IexHistCapacityDisposition, IexHistCapacityError, IexHistCapacityFootprint,
    IexHistCapacityLease, IexHistCapacityRequest, IexHistCapacitySettlement,
    IexHistCheckpointStore, IexHistCheckpointStoreError, IexHistDownloadOutcome,
    IexHistDurableJob, IexHistJobPhase, IexHistPlanner, IexHistReactivationRequirement,
    IexHistRecoveryAction, IexHistResumeAdoptionRequest, IexHistResumeCandidate, IexHistResumeCause,
    IexHistResumePhysicalAdopter, IexHistRetryDisposition, IexHistSharedPhysicalSealReceipt,
    IexHistTerminalCoordinate, IexHistTerminalDisposition, IexHistTerminalError,
    IexHistTerminalPhase, IexHistTrustedClockReading, IexHistTypedHandoffBuilder,
    PcapObjectEncoding, ResumePolicy,
    ScheduleLane, Sha256Digest, TradeDate, TransportErrorKind, TransportVersion,
};

const OBSERVED_ON: &str = "20260811";
const TRADE_DATE: &str = "20260810";
const FILE_NAME: &str = "20260810_IEXTP1_TOPS1.6.pcap.gz";
const DOWNLOAD_URL: &str = "https://www.googleapis.com/download/storage/v1/b/iex/o/data%2Ffeeds%2F20260810%2F20260810_IEXTP1_TOPS1.6.pcap.gz?generation=1786415919114081&alt=media";
const AUTHORITY_NOW: i64 = 1_786_425_600_000_000_000;
const ATTEMPT_DEADLINE: i64 = AUTHORITY_NOW + 30_000_000_000;
const STRONG_ETAG: &str = "\"fixture-object-v1\"";

#[tokio::test]
async fn selected_feed_date_resumes_decodes_and_hands_off_native_bars() {
    let pcap = build_valid_pcap();
    let gzip = stored_gzip(&pcap);
    let staging = tempfile::tempdir().unwrap();
    let authority = MemoryCapacityAuthority::new(staging.path());
    let catalog = parse_catalog(
        &catalog_body(u64::try_from(gzip.len()).unwrap()),
        &authority,
    );
    let selected = catalog
        .select(&ExactFileRequest {
            trade_date: TradeDate::parse(TRADE_DATE).unwrap(),
            feed: FeedKind::Tops,
            feed_version: FeedVersion::Tops1_6,
            transport_version: TransportVersion::IexTp1,
            object_encoding: PcapObjectEncoding::Gzip,
            file_name: FILE_NAME.to_owned(),
        })
        .unwrap();
    let plan = plan(selected, 8_192);
    assert_eq!(plan.lane(), ScheduleLane::Cold);
    assert!(!plan.automatic_archive_catch_up());
    assert_eq!(plan.max_parallel_transfers(), 1);
    assert_eq!(
        plan.resume_policy(),
        ResumePolicy::VerifiedStrongValidatorRangeOrRestartWholeFile
    );
    assert!(plan.required_disk_bytes().unwrap() > u64::try_from(pcap.len()).unwrap());
    assert_eq!(
        IexHistPlanner::restore(&plan.durable_envelope().unwrap()).unwrap(),
        plan
    );
    assert!(crate::transport::exact_content_range_matches(
        Some("bytes 10-99/100"),
        10,
        100
    ));
    assert!(!crate::transport::exact_content_range_matches(
        Some("bytes 9-99/100"),
        10,
        100
    ));

    let split = gzip.len() / 2;
    assert!(split > 0 && split < gzip.len());
    let initial = materialize_mock_stream(
        &plan,
        response_metadata(200, 0, u64::try_from(gzip.len()).unwrap(), STRONG_ETAG),
        vec![MockStreamChunk::Bytes(Bytes::copy_from_slice(
            &gzip[..split],
        ))],
        acquire_permit(&plan, &authority, ATTEMPT_DEADLINE),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let pending = match initial {
        IexHistDownloadOutcome::ResumePending(pending) => pending,
        IexHistDownloadOutcome::Materialized(_) => panic!("truncated stream completed"),
    };
    assert_eq!(pending.cause(), IexHistResumeCause::Network);
    assert_eq!(
        pending.claim().prefix_bytes(),
        u64::try_from(split).unwrap()
    );
    assert_eq!(
        pending.claim().prefix_sha256(),
        Sha256Digest::of(&gzip[..split])
    );
    assert_eq!(pending.claim().strong_etag(), STRONG_ETAG);
    assert_eq!(
        pending.telemetry().response_bytes(),
        u64::try_from(split).unwrap()
    );
    let mut adopter = PhysicalPrefixFixtureAdopter;
    let adopted = pending.try_adopt(&plan, &mut adopter).unwrap();
    let (adoption, prefix) = adopted.into_parts();
    let claim = adoption.claim().clone();
    assert_eq!(adoption.cause(), IexHistResumeCause::Network);
    assert_eq!(adoption.telemetry().network_failures_total(), 1);
    assert_eq!(prefix.object_sha256(), claim.prefix_sha256());

    let rejected = resume_mock_stream(
        &plan,
        response_metadata(200, 0, u64::try_from(gzip.len()).unwrap(), STRONG_ETAG),
        vec![MockStreamChunk::Bytes(Bytes::copy_from_slice(
            &gzip[split..],
        ))],
        acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 1),
        &CancellationToken::new(),
        IexHistResumeCandidate::new(adoption.clone(), prefix.reopen().unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        rejected.kind(),
        TransportErrorKind::Capture(CaptureError::InvalidResponseMetadata)
    ));

    let suffix_bytes = u64::try_from(gzip.len() - split).unwrap();
    let resumed = resume_mock_stream(
        &plan,
        response_metadata(
            206,
            u64::try_from(split).unwrap(),
            suffix_bytes,
            STRONG_ETAG,
        ),
        gzip[split..]
            .chunks(11)
            .map(|chunk| MockStreamChunk::Bytes(Bytes::copy_from_slice(chunk)))
            .collect(),
        acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 2),
        &CancellationToken::new(),
        IexHistResumeCandidate::new(adoption.clone(), prefix.reopen().unwrap()),
    )
    .await
    .unwrap();
    let materialized = match resumed {
        IexHistDownloadOutcome::Materialized(materialized) => materialized,
        IexHistDownloadOutcome::ResumePending(_) => panic!("complete suffix stayed pending"),
    };
    assert_eq!(materialized.telemetry().response_bytes(), suffix_bytes);
    assert_eq!(
        materialized.telemetry().staged_provider_object_bytes(),
        u64::try_from(gzip.len()).unwrap()
    );
    let (capture, _, files, materialize_permit) = materialized.into_parts();
    assert_eq!(
        capture.chronology_disposition(),
        CaptureChronologyDisposition::Admitted
    );
    assert_eq!(capture.resume_adoption(), Some(&adoption));
    assert_eq!(
        capture.attempt().deadline_unix_nanos(),
        ATTEMPT_DEADLINE + 2
    );
    assert_eq!(capture.response_status(), 206);
    assert_eq!(
        capture.response_range_start(),
        u64::try_from(split).unwrap()
    );
    assert_eq!(capture.response_content_length(), suffix_bytes);
    assert_eq!(capture.compressed_sha256(), Sha256Digest::of(&gzip));
    assert_eq!(capture.pcap_sha256(), Sha256Digest::of(&pcap));
    drop(materialize_permit);

    let store = MemoryCheckpointStore::default();
    let mut durable = IexHistDurableJob::try_open(&plan, store.clone()).unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::Planned);
    durable
        .record_capture(&plan, capture.clone(), trusted_clock(AUTHORITY_NOW + 3))
        .unwrap();
    drop(durable);
    let mut durable = IexHistDurableJob::restore(store.clone()).unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::CaptureEvidence);
    assert_eq!(
        durable.recovery_action(),
        IexHistRecoveryAction::RequireSharedArtifactAdoptionOrRestartWholeFile
    );
    assert_eq!(durable.capture_evidence(), Some(&capture));

    let decoded = crate::transport::decode_mock_pcap(
        &plan,
        &capture,
        files.reopen_pcap().unwrap(),
        acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 3),
        &CancellationToken::new(),
        IexHistTypedHandoffBuilder::try_new(&plan, &capture).unwrap(),
    )
    .await
    .unwrap();
    let (summary, builder, telemetry, decode_permit) = decoded.into_parts();
    assert_eq!(summary.messages, 6);
    assert_eq!(
        telemetry.staged_decoded_event_batch_bytes(),
        summary.decoded_event_batch_bytes
    );
    let durable_summary = summary.clone();
    let handoff = builder.try_into_handoff(summary).unwrap();
    assert_eq!(handoff.events().len(), 6);
    assert_eq!(handoff.summary(), &durable_summary);
    for event in handoff.events() {
        assert_eq!(
            event.native_serialized_sha256(),
            Sha256Digest::of(event.native_serialized_bytes())
        );
    }
    assert!(matches!(
        &handoff.events()[1].decoded_event().event,
        IexEvent::Quote { symbol, .. } if symbol == "AAPL"
    ));
    assert!(matches!(
        &handoff.events()[2].decoded_event().event,
        IexEvent::Trade { symbol, size: 25, trade_id: 42, .. } if symbol == "AAPL"
    ));
    assert!(matches!(
        &handoff.events()[3].decoded_event().event,
        IexEvent::Trade {
            symbol,
            sale_condition_flags: 0x20,
            size: 40,
            trade_id: 43,
            ..
        } if symbol == "AAPL"
    ));
    assert!(matches!(
        &handoff.events()[4].decoded_event().event,
        IexEvent::TradeBreak {
            symbol,
            sale_condition_flags: 0x20,
            size: 40,
            trade_id: 43,
            ..
        } if symbol == "AAPL"
    ));
    let bars = handoff
        .try_into_derived_bars(IexHistBarInterval::OneMinute)
        .unwrap();
    assert_eq!(bars.bars().len(), 1);
    let bar = &bars.bars()[0];
    assert_eq!(bar.symbol(), "AAPL");
    assert_eq!(bar.open().value(), 1_900_050);
    assert_eq!(bar.high(), bar.open());
    assert_eq!(bar.low(), bar.open());
    assert_eq!(bar.close(), bar.open());
    assert_eq!(bar.volume(), 25);
    assert_eq!(bar.trade_count(), 1);
    assert_eq!(
        bar.source_provider_content_sha256(),
        bars.source().provider_content_sha256()
    );

    durable
        .record_decoded(&plan, &capture, durable_summary.clone())
        .unwrap();
    drop(durable);
    let mut durable = IexHistDurableJob::restore(store.clone()).unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::DecodeEvidence);
    assert_eq!(durable.decode_evidence(), Some(&durable_summary));
    durable
        .record_terminal(
            &plan,
            trusted_clock(AUTHORITY_NOW + 4),
            IexHistTerminalDisposition::Unavailable,
            IexHistTerminalPhase::Recovery,
            IexHistTerminalError::AuthorityUnavailable,
            IexHistTerminalCoordinate::try_new(None, None, None).unwrap(),
            IexHistRetryDisposition::ReacquireAndRestartWholeFile {
                not_before_unix_nanos: AUTHORITY_NOW + 5,
            },
        )
        .unwrap();
    assert_eq!(
        durable.terminal_evidence().unwrap().reactivation(),
        IexHistReactivationRequirement::NewAuthorityAttempt
    );
    drop(durable);
    let durable = IexHistDurableJob::restore(store).unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::Unavailable);
    assert_eq!(
        durable.terminal_evidence().unwrap().attempt_sha256(),
        Some(durable_summary.decode_attempt_sha256)
    );
    drop(decode_permit);

    assert!(
        authority
            .settlements
            .lock()
            .unwrap()
            .iter()
            .all(|settlement| settlement.disposition() != IexHistCapacityDisposition::Completed)
    );
}

#[derive(Clone, Default)]
struct MemoryCheckpointStore(Arc<Mutex<Option<Vec<u8>>>>);

impl IexHistCheckpointStore for MemoryCheckpointStore {
    fn load(&self) -> Result<Option<Vec<u8>>, IexHistCheckpointStoreError> {
        self.0
            .lock()
            .map(|value| value.clone())
            .map_err(|_| IexHistCheckpointStoreError::Unavailable)
    }

    fn compare_and_swap(
        &self,
        expected_payload_sha256: Option<Sha256Digest>,
        next_payload: &[u8],
    ) -> Result<(), IexHistCheckpointStoreError> {
        let mut value = self
            .0
            .lock()
            .map_err(|_| IexHistCheckpointStoreError::Unavailable)?;
        if value.as_deref().map(Sha256Digest::of) != expected_payload_sha256 {
            return Err(IexHistCheckpointStoreError::Conflict);
        }
        *value = Some(next_payload.to_vec());
        Ok(())
    }
}

struct PhysicalPrefixFixtureAdopter;

struct PhysicalPrefixFixtureReceipt {
    provider_object: tempfile::NamedTempFile,
    storage_root_sha256: Sha256Digest,
    object_sha256: Sha256Digest,
    object_bytes: u64,
    physical_receipt_sha256: Sha256Digest,
}

impl PhysicalPrefixFixtureReceipt {
    fn reopen(&self) -> std::io::Result<std::fs::File> {
        self.provider_object.reopen()
    }
}

impl IexHistSharedPhysicalSealReceipt for PhysicalPrefixFixtureReceipt {
    fn storage_root_sha256(&self) -> Sha256Digest {
        self.storage_root_sha256
    }

    fn object_sha256(&self) -> Sha256Digest {
        self.object_sha256
    }

    fn object_bytes(&self) -> u64 {
        self.object_bytes
    }

    fn physical_receipt_sha256(&self) -> Sha256Digest {
        self.physical_receipt_sha256
    }
}

impl IexHistResumePhysicalAdopter for PhysicalPrefixFixtureAdopter {
    type Receipt = PhysicalPrefixFixtureReceipt;
    type Error = std::io::Error;

    fn adopt(
        &mut self,
        request: IexHistResumeAdoptionRequest,
    ) -> Result<Self::Receipt, Self::Error> {
        let (claim, mut provider_object, cause, telemetry) = request.into_parts();
        assert_eq!(cause, IexHistResumeCause::Network);
        assert_eq!(telemetry.network_failures_total(), 1);
        provider_object.as_file().sync_all()?;
        provider_object.as_file_mut().seek(SeekFrom::Start(0))?;
        let mut exact_prefix = Vec::new();
        exact_prefix
            .try_reserve_exact(usize::try_from(claim.prefix_bytes()).unwrap())
            .unwrap();
        provider_object
            .as_file_mut()
            .read_to_end(&mut exact_prefix)?;
        let object_sha256 = Sha256Digest::of(&exact_prefix);
        let object_bytes = u64::try_from(exact_prefix.len()).unwrap();
        let storage_root_sha256 = Sha256Digest::of(b"fixture-storage-root");
        let physical_receipt_sha256 = crate::catalog::digest_fields(&[
            b"fixture-shared-physical-prefix-receipt",
            storage_root_sha256.as_bytes(),
            object_sha256.as_bytes(),
            &object_bytes.to_le_bytes(),
        ]);
        Ok(PhysicalPrefixFixtureReceipt {
            provider_object,
            storage_root_sha256,
            object_sha256,
            object_bytes,
            physical_receipt_sha256,
        })
    }
}

fn response_metadata(
    status: u16,
    range_start: u64,
    content_length: u64,
    etag: &str,
) -> CaptureResponseMetadata {
    CaptureResponseMetadata {
        response_url: DOWNLOAD_URL.to_owned(),
        status,
        range_start,
        content_length,
        content_encoding: Some("identity".to_owned()),
        etag: Some(etag.to_owned()),
        response_started_clock: trusted_clock(AUTHORITY_NOW),
    }
}

#[derive(Clone)]
struct MemoryCapacityAuthority {
    staging: PathBuf,
    settlements: Arc<Mutex<Vec<IexHistCapacitySettlement>>>,
}

impl MemoryCapacityAuthority {
    fn new(staging: &Path) -> Self {
        Self {
            staging: staging.to_path_buf(),
            settlements: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl IexHistCapacityAuthority for MemoryCapacityAuthority {
    fn required_free_reserve_bytes(&self) -> Result<u64, IexHistCapacityError> {
        Ok(1_024)
    }

    fn acquire(
        &self,
        request: &IexHistCapacityRequest,
    ) -> Result<Box<dyn IexHistCapacityLease>, IexHistCapacityError> {
        Ok(Box::new(MemoryCapacityLease {
            request_sha256: request.request_sha256(),
            reservation_sha256: crate::catalog::digest_fields(&[
                b"fixture-iex-capacity-reservation",
                request.request_sha256().as_bytes(),
            ]),
            footprint: request.footprint(),
            deadline_unix_nanos: request.deadline_unix_nanos(),
            staging: self.staging.clone(),
            settlements: Arc::clone(&self.settlements),
        }))
    }
}

struct MemoryCapacityLease {
    request_sha256: Sha256Digest,
    reservation_sha256: Sha256Digest,
    footprint: IexHistCapacityFootprint,
    deadline_unix_nanos: i64,
    staging: PathBuf,
    settlements: Arc<Mutex<Vec<IexHistCapacitySettlement>>>,
}

impl IexHistCapacityLease for MemoryCapacityLease {
    fn request_sha256(&self) -> Sha256Digest {
        self.request_sha256
    }

    fn reservation_sha256(&self) -> Sha256Digest {
        self.reservation_sha256
    }

    fn authority_generation(&self) -> u64 {
        1
    }

    fn storage_root_sha256(&self) -> Sha256Digest {
        Sha256Digest::of(b"fixture-storage-root")
    }

    fn max_parallel_transfers(&self) -> u8 {
        1
    }

    fn reserved_footprint(&self) -> IexHistCapacityFootprint {
        self.footprint
    }

    fn admitted_clock_sample(&self) -> IexHistAuthorityClockSample {
        authority_clock(AUTHORITY_NOW)
    }

    fn deadline_unix_nanos(&self) -> i64 {
        self.deadline_unix_nanos
    }

    fn staging_directory(&self) -> Option<&Path> {
        Some(&self.staging)
    }

    fn trusted_clock_sample(&self) -> Result<IexHistAuthorityClockSample, IexHistCapacityError> {
        Ok(authority_clock(AUTHORITY_NOW))
    }

    fn settle(
        self: Box<Self>,
        settlement: &IexHistCapacitySettlement,
    ) -> Result<(), IexHistCapacityError> {
        self.settlements
            .lock()
            .map_err(|_| IexHistCapacityError::Settlement)?
            .push(settlement.clone());
        Ok(())
    }
}

fn acquire_permit(
    plan: &ColdJobPlan,
    authority: &MemoryCapacityAuthority,
    deadline_unix_nanos: i64,
) -> crate::IexHistExecutionPermit {
    let request = IexHistCapacityRequest::selected_file(plan, deadline_unix_nanos, 1_024).unwrap();
    crate::IexHistExecutionPermit::acquire(authority, request, Some(plan)).unwrap()
}

fn trusted_clock(unix_nanos: i64) -> IexHistTrustedClockReading {
    IexHistTrustedClockReading::try_from(authority_clock(unix_nanos)).unwrap()
}

fn authority_clock(unix_nanos: i64) -> IexHistAuthorityClockSample {
    IexHistAuthorityClockSample {
        unix_nanos,
        utc_offset_seconds: 0,
        observed_date: TradeDate::parse(OBSERVED_ON).unwrap(),
    }
}

fn parse_catalog(body: &[u8], authority: &MemoryCapacityAuthority) -> Catalog {
    let body_bytes = u64::try_from(body.len()).unwrap();
    let footprint = IexHistCapacityFootprint::catalog(body_bytes, 1_024, 1_024).unwrap();
    let request = IexHistCapacityRequest::catalog(footprint, ATTEMPT_DEADLINE).unwrap();
    let permit = crate::IexHistExecutionPermit::acquire(authority, request, None).unwrap();
    let observation = permit.observe_catalog_body(body).unwrap();
    let catalog = Catalog::parse(
        body,
        CatalogTransportMetadata {
            status: 200,
            content_type: "application/json;charset=utf-8".to_owned(),
            content_length: body_bytes,
            etag: Some("W/\"fixture\"".to_owned()),
            observation,
        },
    )
    .unwrap();
    drop(permit);
    catalog
}

fn plan(selected: crate::SelectedFileReceipt, max_pcap_bytes: u64) -> ColdJobPlan {
    IexHistPlanner::plan(
        selected,
        ColdJobTrigger::ResearchJob,
        ByteAdmissionLimits {
            max_compressed_bytes: 2_000,
            max_pcap_bytes,
            max_decoded_event_batch_bytes: 8_192,
            max_canonical_arrow_bytes: 8_192,
            max_parquet_bytes: 8_192,
            manifest_and_atomic_overhead_bytes: 1_024,
            required_free_reserve_bytes: 1_024,
            max_catalog_age_nanos: 3_600_000_000_000,
            max_download_duration_nanos: 60_000_000_000,
            max_clock_regression_nanos: 1_000_000,
        },
        DecodeLimits {
            max_stream_chunk_bytes: 8_192,
            max_packet_bytes: 2_048,
            max_packets: 8,
            max_messages: 8,
            max_decoded_event_batch_bytes: 8_192,
            max_timestamp_keys: 8,
            max_send_capture_skew_nanos: 1_000_000,
        },
        None,
    )
    .unwrap()
}

fn catalog_body(size: u64) -> Vec<u8> {
    format!(
        "{{\"{TRADE_DATE}\":[{{\"link\":\"{DOWNLOAD_URL}\",\"date\":\"{TRADE_DATE}\",\"feed\":\"TOPS\",\"version\":\"1.6\",\"protocol\":\"IEXTP1\",\"size\":\"{size}\"}}]}}"
    )
    .into_bytes()
}

fn build_valid_pcap() -> Vec<u8> {
    let date = TradeDate::parse(TRADE_DATE).unwrap();
    let base = date.start_epoch_nanos().unwrap() + 50_000_000_000_000;
    let start = system_message(b'O', base);
    let quote = quote_message(base + 1_000);
    let trade = trade_message(b'T', 0, base + 2_000, 25, 1_900_050, 42);
    let broken_trade = trade_message(b'T', 0x20, base + 3_000, 40, 1_900_200, 43);
    let trade_break = trade_message(b'B', 0x20, base + 4_000, 40, 1_900_200, 43);
    let end = system_message(b'C', base + 5_000);
    let first = iex_segment(&[start, quote, trade, broken_trade], 0, 1, base + 3_500);
    let second_offset = i64::try_from(first.len() - 40).unwrap();
    let second = iex_segment(&[trade_break, end], second_offset, 5, base + 5_500);
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

fn trade_message(
    message_type: u8,
    sale_condition_flags: u8,
    timestamp: i64,
    size: u32,
    price: i64,
    trade_id: i64,
) -> Vec<u8> {
    let mut message = vec![message_type, sale_condition_flags];
    message.extend_from_slice(&timestamp.to_le_bytes());
    message.extend_from_slice(b"AAPL    ");
    message.extend_from_slice(&size.to_le_bytes());
    message.extend_from_slice(&price.to_le_bytes());
    message.extend_from_slice(&trade_id.to_le_bytes());
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
        payload.extend_from_slice(&u16::try_from(message.len()).unwrap().to_le_bytes());
        payload.extend_from_slice(message);
    }
    let mut segment = vec![1, 0];
    segment.extend_from_slice(&0x8003_u16.to_le_bytes());
    segment.extend_from_slice(&1_u32.to_le_bytes());
    segment.extend_from_slice(&7_u32.to_le_bytes());
    segment.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
    segment.extend_from_slice(&u16::try_from(messages.len()).unwrap().to_le_bytes());
    segment.extend_from_slice(&stream_offset.to_le_bytes());
    segment.extend_from_slice(&first_sequence.to_le_bytes());
    segment.extend_from_slice(&send_time.to_le_bytes());
    segment.extend_from_slice(&payload);
    segment
}

fn ethernet_udp(payload: &[u8]) -> Vec<u8> {
    let udp_length = u16::try_from(8 + payload.len()).unwrap();
    let ip_length = u16::try_from(20 + usize::from(udp_length)).unwrap();
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
    let length = udp_length.to_be_bytes();
    let pseudo = [10, 0, 0, 1, 233, 215, 21, 3, 0, 17, length[0], length[1]];
    let mut udp_checksum = checksum(&[&pseudo, &frame[udp_start..]]);
    if udp_checksum == 0 {
        udp_checksum = 0xffff;
    }
    frame[udp_start + 6..udp_start + 8].copy_from_slice(&udp_checksum.to_be_bytes());
    frame
}

fn append_record(pcap: &mut Vec<u8>, frame: &[u8], capture_nanos: i64) {
    let seconds = u32::try_from(capture_nanos / 1_000_000_000).unwrap();
    let nanos = u32::try_from(capture_nanos % 1_000_000_000).unwrap();
    let length = u32::try_from(frame.len()).unwrap();
    pcap.extend_from_slice(&seconds.to_le_bytes());
    pcap.extend_from_slice(&nanos.to_le_bytes());
    pcap.extend_from_slice(&length.to_le_bytes());
    pcap.extend_from_slice(&length.to_le_bytes());
    pcap.extend_from_slice(frame);
}

fn stored_gzip(payload: &[u8]) -> Vec<u8> {
    let length = u16::try_from(payload.len()).unwrap();
    let mut gzip = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255, 1];
    gzip.extend_from_slice(&length.to_le_bytes());
    gzip.extend_from_slice(&(!length).to_le_bytes());
    gzip.extend_from_slice(payload);
    let mut crc = Crc32::new();
    crc.update(payload);
    gzip.extend_from_slice(&crc.finalize().to_le_bytes());
    gzip.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
    gzip
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
