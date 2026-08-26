#![allow(
    clippy::panic,
    clippy::unwrap_used,
    reason = "the three closed synthetic proofs terminate immediately when fixture construction fails"
)]

use crc32fast::Hasher as Crc32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::{
    ByteAdmissionLimits, CaptureChronologyDisposition, Catalog, ColdJobPlan, ColdJobTrigger,
    DecodeError, DecodeLimits, ExactFileRequest, FeedKind, FeedVersion, IexEventSink,
    IexHistAuthorityClockSample, IexHistCapacityAuthority, IexHistCapacityCategory,
    IexHistCapacityDisposition, IexHistCapacityError, IexHistCapacityFootprint,
    IexHistCapacityLease, IexHistCapacityRequest,
    IexHistCapacitySettlement, IexHistCheckpointStore, IexHistCheckpointStoreError,
    IexHistDurableJob, IexHistJobPhase, IexHistPlanner, IexHistReactivationRequirement,
    IexHistRecoveryAction, IexHistRetryDisposition, IexHistTerminalCoordinate,
    IexHistTerminalDisposition, IexHistTerminalError, IexHistTerminalPhase,
    IexHistTrustedClockReading, PcapMaterializationReceipt, PcapObjectEncoding,
    PcapStreamDecoder, ScheduleLane, Sha256Digest, TradeDate, TransportVersion,
};
use crate::catalog::CatalogTransportMetadata;
use crate::receipt::{CaptureResponseMetadata, GzipPcapReceiptBuilder};

const OBSERVED_ON: &str = "20260811";
const TRADE_DATE: &str = "20260810";
const FILE_NAME: &str = "20260810_IEXTP1_TOPS1.6.pcap.gz";
const DOWNLOAD_URL: &str = "https://www.googleapis.com/download/storage/v1/b/iex/o/data%2Ffeeds%2F20260810%2F20260810_IEXTP1_TOPS1.6.pcap.gz?generation=1786415919114081&alt=media";
const AUTHORITY_NOW: i64 = 1_786_425_600_000_000_000;
const ATTEMPT_DEADLINE: i64 = AUTHORITY_NOW + 30_000_000_000;

#[test]
fn catalog_receipt_and_exact_cold_byte_plan_are_restorable() {
    let body = catalog_body(1_000);
    let staging = tempfile::tempdir().unwrap();
    let authority = MemoryCapacityAuthority::new(staging.path());
    let catalog = parse_catalog(&body, &authority);
    assert_eq!(catalog.receipt().body_sha256, Sha256Digest::of(&body));
    assert_eq!(catalog.receipt().date_count, 1);
    assert_eq!(catalog.receipt().file_count, 1);
    assert_eq!(catalog.receipt().advertised_compressed_bytes, 1_000);
    assert_eq!(catalog.receipt().observation.body_sha256(), Sha256Digest::of(&body));
    let settlements = authority.settlements.lock().unwrap();
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].disposition(), IexHistCapacityDisposition::Completed);
    assert_eq!(
        settlements[0].usage().bytes(IexHistCapacityCategory::NetworkResponse),
        u64::try_from(body.len()).unwrap()
    );
    assert_eq!(
        settlements[0].usage().bytes(IexHistCapacityCategory::DurableCatalog),
        u64::try_from(body.len()).unwrap()
    );
    drop(settlements);

    let selected = select_tops(&catalog);
    assert_eq!(selected.object_encoding(), PcapObjectEncoding::Gzip);
    let plan = plan(selected, 4_000);
    assert_eq!(plan.lane(), ScheduleLane::Cold);
    assert!(!plan.automatic_archive_catch_up());
    assert_eq!(plan.max_parallel_transfers(), 1);
    assert_eq!(plan.earliest_available_on().compact(), OBSERVED_ON);
    assert_eq!(plan.rolling_window_start().compact(), "20250811");
    assert!(plan.required_disk_bytes().unwrap() > 4_000);

    let restored = IexHistPlanner::restore(&plan.durable_envelope().unwrap()).unwrap();
    assert_eq!(restored, plan);
}

#[test]
fn synthetic_tops_sequence_commits_transactionally_and_refuses_gap_and_corruption() {
    let valid_pcap = build_valid_pcap();
    let valid_gzip = stored_gzip(&valid_pcap);
    let staging = tempfile::tempdir().unwrap();
    let authority = MemoryCapacityAuthority::new(staging.path());
    let plan = fixture_plan(
        u64::try_from(valid_gzip.len()).unwrap_or(u64::MAX),
        8_192,
        &authority,
    );
    let receipt = capture_receipt(&plan, &valid_gzip, &valid_pcap, &authority, AUTHORITY_NOW);
    let committed = Arc::new(Mutex::new(Vec::new()));
    let sink = TransactionalSink::new(Arc::clone(&committed));
    let mut decode_permit = acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 1);
    let decode_attempt = decode_permit.decode_attempt_evidence(&plan).unwrap();
    let mut decoder = PcapStreamDecoder::new(
        &plan,
        &receipt,
        decode_attempt,
        sink,
    )
    .unwrap_or_else(|failure| panic!("decoder setup failed: {:?}", failure.error()));
    for chunk in valid_pcap.chunks(37) {
        decoder
            .push(chunk)
            .unwrap_or_else(|failure| panic!("valid PCAP failed: {:?}", failure.error()));
    }
    let (summary, sink) = decoder
        .finish()
        .unwrap_or_else(|failure| panic!("valid PCAP did not finish: {:?}", failure.error()));
    assert_eq!(summary.messages, 3);
    assert_eq!(summary.decode_contract, plan.decode_contract());
    assert_eq!(summary.decode_attempt_evidence, decode_attempt);
    assert_eq!(summary.channel_sessions.len(), 1);
    assert_eq!(summary.channel_sessions[0].next_sequence, 4);
    assert!(sink.committed);
    let events = committed.lock().unwrap();
    assert_eq!(events.len(), 3);
    let quote = std::str::from_utf8(&events[1]).unwrap();
    assert!(quote.contains("\"kind\":\"quote\""));
    assert!(quote.contains("\"symbol\":\"AAPL\""));
    drop(events);
    let actuals = summary.actuals();
    decode_permit
        .record_usage(IexHistCapacityCategory::DurablePcap, actuals.pcap_bytes_read())
        .unwrap();
    decode_permit
        .record_usage(
            IexHistCapacityCategory::DecodedEventBatch,
            actuals.decoded_event_batch_bytes_staged(),
        )
        .unwrap();
    let decode_attempt_sha256 = decode_permit.attempt().attempt_sha256();
    drop(decode_permit);
    let settlement = authority
        .settlements
        .lock()
        .unwrap()
        .iter()
        .find(|settlement| settlement.attempt_sha256() == decode_attempt_sha256)
        .cloned()
        .unwrap();
    assert_eq!(settlement.disposition(), IexHistCapacityDisposition::Interrupted);
    assert_eq!(
        settlement.usage().bytes(IexHistCapacityCategory::DecodedEventBatch),
        summary.decoded_event_batch_bytes
    );

    let mut gapped = valid_pcap.clone();
    rewrite_second_sequence(&mut gapped, 4);
    let gapped_gzip = stored_gzip(&gapped);
    let gapped_receipt = capture_receipt(
        &plan,
        &gapped_gzip,
        &gapped,
        &authority,
        AUTHORITY_NOW + 1,
    );
    let aborted = Arc::new(Mutex::new(Vec::new()));
    let gap_permit = acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 2);
    let gap_attempt = gap_permit.decode_attempt_evidence(&plan).unwrap();
    let mut decoder = PcapStreamDecoder::new(
        &plan,
        &gapped_receipt,
        gap_attempt,
        TransactionalSink::new(Arc::clone(&aborted)),
    )
    .unwrap_or_else(|failure| panic!("gap decoder setup failed: {:?}", failure.error()));
    let failure = decoder.push(&gapped).unwrap_err();
    assert!(matches!(
        failure.error(),
        DecodeError::SequenceGap {
            expected: 3,
            actual: 4
        }
    ));
    assert_eq!(failure.actuals().pcap_bytes_read(), u64::try_from(gapped.len()).unwrap());
    drop(gap_permit);
    drop(decoder);
    assert!(aborted.lock().unwrap().is_empty());

    let mut corrupted = valid_pcap;
    let quote_price_offset = first_packet_data_offset(&corrupted) + 40 + 2 + 10 + 2 + 22;
    corrupted[quote_price_offset] ^= 0x01;
    let corrupt_gzip = stored_gzip(&corrupted);
    let corrupt_receipt = capture_receipt(
        &plan,
        &corrupt_gzip,
        &corrupted,
        &authority,
        AUTHORITY_NOW + 2,
    );
    let corrupt_permit = acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 3);
    let corrupt_attempt = corrupt_permit.decode_attempt_evidence(&plan).unwrap();
    let mut decoder = PcapStreamDecoder::new(
        &plan,
        &corrupt_receipt,
        corrupt_attempt,
        TransactionalSink::new(Arc::new(Mutex::new(Vec::new()))),
    )
    .unwrap_or_else(|failure| panic!("corrupt decoder setup failed: {:?}", failure.error()));
    let failure = decoder.push(&corrupted).unwrap_err();
    assert_eq!(failure.error(), &DecodeError::InvalidUdpChecksum);
    assert_eq!(failure.actuals().pcap_bytes_read(), u64::try_from(corrupted.len()).unwrap());
    drop(corrupt_permit);
}

#[tokio::test]
async fn selected_file_evidence_restores_and_terminal_recovery_is_typed() {
    let pcap = build_valid_pcap();
    let gzip = stored_gzip(&pcap);
    let staging = tempfile::tempdir().unwrap();
    let authority = MemoryCapacityAuthority::new(staging.path());
    let plan = fixture_plan(
        u64::try_from(gzip.len()).unwrap_or(u64::MAX),
        8_192,
        &authority,
    );
    let chunks = gzip.chunks(13).map(Bytes::copy_from_slice).collect();
    let permit = acquire_permit(&plan, &authority, ATTEMPT_DEADLINE);
    let outcome = crate::transport::materialize_mock_stream(
        &plan,
        CaptureResponseMetadata {
            response_url: DOWNLOAD_URL.to_owned(),
            status: 200,
            content_length: u64::try_from(gzip.len()).unwrap_or(u64::MAX),
            content_encoding: Some("identity".to_owned()),
            etag: Some("mock-stream".to_owned()),
            response_started_clock: trusted_clock(AUTHORITY_NOW),
        },
        chunks,
        permit,
        &CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("mock selected-file stream failed: {error}"));

    assert_eq!(outcome.telemetry().attempts_total(), 1);
    assert_eq!(outcome.telemetry().response_bytes(), u64::try_from(gzip.len()).unwrap());
    assert_eq!(outcome.telemetry().expanded_pcap_bytes(), u64::try_from(pcap.len()).unwrap());
    let (capture, _, files, materialize_permit) = outcome.into_parts();
    assert_eq!(capture.chronology_disposition(), CaptureChronologyDisposition::Admitted);
    let materialize_attempt_sha256 = materialize_permit.attempt().attempt_sha256();
    drop(materialize_permit);
    let materialize_settlement = authority
        .settlements
        .lock()
        .unwrap()
        .iter()
        .find(|settlement| settlement.attempt_sha256() == materialize_attempt_sha256)
        .cloned()
        .unwrap();
    assert_eq!(
        materialize_settlement.disposition(),
        IexHistCapacityDisposition::Interrupted
    );
    assert_eq!(
        materialize_settlement
            .usage()
            .bytes(IexHistCapacityCategory::NetworkResponse),
        u64::try_from(gzip.len()).unwrap()
    );
    assert_eq!(
        materialize_settlement
            .usage()
            .bytes(IexHistCapacityCategory::TemporaryPcap),
        u64::try_from(pcap.len()).unwrap()
    );

    let store = MemoryCheckpointStore::default();
    let mut durable = IexHistDurableJob::try_open(&plan, store.clone()).unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::Planned);
    durable
        .record_capture(&plan, capture.clone(), trusted_clock(AUTHORITY_NOW))
        .unwrap();
    assert_eq!(durable.state_version(), 2);
    drop(durable);

    let mut durable = IexHistDurableJob::restore(store.clone()).unwrap();
    assert_eq!(durable.plan(), &plan);
    assert_eq!(durable.phase(), IexHistJobPhase::CaptureEvidence);
    assert_eq!(
        durable.recovery_action(),
        IexHistRecoveryAction::RequireSharedArtifactAdoptionOrRestartWholeFile
    );

    let committed = Arc::new(Mutex::new(Vec::new()));
    let decode_permit = acquire_permit(&plan, &authority, ATTEMPT_DEADLINE + 1);
    let decoded = crate::transport::decode_mock_pcap(
        &plan,
        &capture,
        files.reopen_pcap().unwrap(),
        decode_permit,
        &CancellationToken::new(),
        TransactionalSink::new(Arc::clone(&committed)),
    )
    .await
    .unwrap_or_else(|error| panic!("materialized PCAP decode failed: {error}"));
    let (summary, sink, decode_telemetry, decode_permit) = decoded.into_parts();
    assert!(sink.committed);
    assert_eq!(summary.messages, 3);
    assert_eq!(summary.decode_contract, plan.decode_contract());
    assert_eq!(
        decode_telemetry.staged_decoded_event_batch_bytes(),
        summary.decoded_event_batch_bytes
    );
    assert_eq!(committed.lock().unwrap().len(), 3);
    durable
        .record_decoded(&plan, &capture, summary.clone())
        .unwrap();
    let decode_attempt_sha256 = decode_permit.attempt().attempt_sha256();
    drop(decode_permit);
    let decode_settlement = authority
        .settlements
        .lock()
        .unwrap()
        .iter()
        .find(|settlement| settlement.attempt_sha256() == decode_attempt_sha256)
        .cloned()
        .unwrap();
    assert_eq!(decode_settlement.disposition(), IexHistCapacityDisposition::Interrupted);
    assert_eq!(
        decode_settlement
            .usage()
            .bytes(IexHistCapacityCategory::DurablePcap),
        u64::try_from(pcap.len()).unwrap()
    );
    assert_eq!(
        decode_settlement
            .usage()
            .bytes(IexHistCapacityCategory::DecodedEventBatch),
        summary.decoded_event_batch_bytes
    );
    drop(durable);

    let mut durable = IexHistDurableJob::restore(store).unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::DecodeEvidence);
    assert_eq!(durable.decode_evidence(), Some(&summary));
    durable
        .record_terminal(
            &plan,
            trusted_clock(AUTHORITY_NOW + 1),
            IexHistTerminalDisposition::Unavailable,
            IexHistTerminalPhase::Recovery,
            IexHistTerminalError::AuthorityUnavailable,
            IexHistTerminalCoordinate::try_new(None, None, None).unwrap(),
            IexHistRetryDisposition::ReacquireAndRestartWholeFile {
                not_before_unix_nanos: AUTHORITY_NOW + 2,
            },
        )
        .unwrap();
    assert_eq!(durable.phase(), IexHistJobPhase::Unavailable);
    assert_eq!(durable.state_version(), 4);
    let terminal = durable.terminal_evidence().unwrap();
    assert_eq!(
        terminal.reactivation(),
        IexHistReactivationRequirement::NewAuthorityAttempt
    );
    assert_eq!(terminal.failed_phase(), IexHistTerminalPhase::Recovery);
    assert_eq!(terminal.error(), IexHistTerminalError::AuthorityUnavailable);
    assert_eq!(terminal.observed_at_unix_nanos(), AUTHORITY_NOW + 1);
    assert_eq!(terminal.attempt_sha256(), Some(summary.decode_attempt_sha256));

    let quarantine_store = MemoryCheckpointStore::default();
    let mut quarantined = IexHistDurableJob::try_open(&plan, quarantine_store.clone()).unwrap();
    let quarantine_authority =
        MemoryCapacityAuthority::new_at(staging.path(), AUTHORITY_NOW + 10_000_000);
    let quarantine_capture = capture_receipt_with_clocks(
        &plan,
        &gzip,
        &pcap,
        &quarantine_authority,
        AUTHORITY_NOW + 10_000_000,
        AUTHORITY_NOW + 1,
        AUTHORITY_NOW + 10_000_001,
    );
    assert!(matches!(
        quarantine_capture.chronology_disposition(),
        CaptureChronologyDisposition::Quarantined(_)
    ));
    quarantined
        .record_capture(
            &plan,
            quarantine_capture,
            trusted_clock(AUTHORITY_NOW + 10_000_001),
        )
        .unwrap();
    assert_eq!(quarantined.phase(), IexHistJobPhase::Quarantined);
    assert_eq!(quarantined.state_version(), 2);
    let terminal = quarantined.terminal_evidence().unwrap();
    assert_eq!(terminal.retry(), IexHistRetryDisposition::Never);
    assert_eq!(terminal.failed_phase(), IexHistTerminalPhase::Capture);
    assert_eq!(terminal.error(), IexHistTerminalError::CaptureClockAnomaly);
    drop(quarantined);
    let quarantined = IexHistDurableJob::restore(quarantine_store).unwrap();
    assert_eq!(quarantined.recovery_action(), IexHistRecoveryAction::AwaitReactivation);
}

#[derive(Clone, Default)]
struct MemoryCheckpointStore(Arc<Mutex<Option<Vec<u8>>>>);

impl IexHistCheckpointStore for MemoryCheckpointStore {
    fn load(&self) -> Result<Option<Vec<u8>>, IexHistCheckpointStoreError> {
        self.0.lock().map(|value| value.clone()).map_err(|_| IexHistCheckpointStoreError::Unavailable)
    }

    fn compare_and_swap(
        &self,
        expected_payload_sha256: Option<Sha256Digest>,
        next_payload: &[u8],
    ) -> Result<(), IexHistCheckpointStoreError> {
        let mut value = self.0.lock().map_err(|_| IexHistCheckpointStoreError::Unavailable)?;
        if value.as_deref().map(Sha256Digest::of) != expected_payload_sha256 {
            return Err(IexHistCheckpointStoreError::Conflict);
        }
        *value = Some(next_payload.to_vec());
        Ok(())
    }
}

#[derive(Clone)]
struct MemoryCapacityAuthority {
    staging: PathBuf,
    admitted_clock: IexHistAuthorityClockSample,
    settlements: Arc<Mutex<Vec<IexHistCapacitySettlement>>>,
}

impl MemoryCapacityAuthority {
    fn new(staging: &Path) -> Self {
        Self::new_at(staging, AUTHORITY_NOW)
    }

    fn new_at(staging: &Path, unix_nanos: i64) -> Self {
        Self {
            staging: staging.to_path_buf(),
            admitted_clock: authority_clock(unix_nanos),
            settlements: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl IexHistCapacityAuthority for MemoryCapacityAuthority {
    fn required_free_reserve_bytes(&self) -> Result<u64, IexHistCapacityError> { Ok(1_024) }

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
            admitted_clock: self.admitted_clock,
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
    admitted_clock: IexHistAuthorityClockSample,
    staging: PathBuf,
    settlements: Arc<Mutex<Vec<IexHistCapacitySettlement>>>,
}

impl IexHistCapacityLease for MemoryCapacityLease {
    fn request_sha256(&self) -> Sha256Digest { self.request_sha256 }
    fn reservation_sha256(&self) -> Sha256Digest { self.reservation_sha256 }
    fn authority_generation(&self) -> u64 { 1 }
    fn storage_root_sha256(&self) -> Sha256Digest { Sha256Digest::of(b"fixture-storage-root") }
    fn max_parallel_transfers(&self) -> u8 { 1 }
    fn reserved_footprint(&self) -> IexHistCapacityFootprint { self.footprint }
    fn admitted_clock_sample(&self) -> IexHistAuthorityClockSample { self.admitted_clock }
    fn deadline_unix_nanos(&self) -> i64 { self.deadline_unix_nanos }
    fn staging_directory(&self) -> Option<&Path> { Some(&self.staging) }
    fn trusted_clock_sample(&self) -> Result<IexHistAuthorityClockSample, IexHistCapacityError> {
        Ok(self.admitted_clock)
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

struct TransactionalSink {
    staged: Vec<Vec<u8>>,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    committed: bool,
}

impl TransactionalSink {
    fn new(published: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        Self { staged: Vec::new(), published, committed: false }
    }
}

impl IexEventSink for TransactionalSink {
    type Error = ();

    fn stage(&mut self, ordinal: u64, serialized_event: &[u8]) -> Result<(), Self::Error> {
        if usize::try_from(ordinal).map_err(|_| ())? != self.staged.len() {
            return Err(());
        }
        self.staged.push(serialized_event.to_vec());
        Ok(())
    }

    fn commit(&mut self, summary: &crate::DecodeSummary) -> Result<Sha256Digest, Self::Error> {
        if u64::try_from(self.staged.len()).map_err(|_| ())? != summary.messages {
            return Err(());
        }
        *self.published.lock().map_err(|_| ())? = self.staged.clone();
        self.committed = true;
        Ok(summary.sink_commit_sha256)
    }

    fn abort(&mut self) {
        self.staged.clear();
        self.committed = false;
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
    let mut permit = crate::IexHistExecutionPermit::acquire(authority, request, None).unwrap();
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
    .unwrap_or_else(|error| panic!("catalog fixture failed: {error}"));
    permit.record_usage(IexHistCapacityCategory::NetworkResponse, body_bytes).unwrap();
    permit.record_usage(IexHistCapacityCategory::DurableCatalog, body_bytes).unwrap();
    permit.settle(IexHistCapacityDisposition::Completed).unwrap();
    catalog
}

fn select_tops(catalog: &Catalog) -> crate::SelectedFileReceipt {
    catalog
        .select(&ExactFileRequest {
            trade_date: TradeDate::parse(TRADE_DATE).unwrap(),
            feed: FeedKind::Tops,
            feed_version: FeedVersion::Tops1_6,
            transport_version: TransportVersion::IexTp1,
            object_encoding: PcapObjectEncoding::Gzip,
            file_name: FILE_NAME.to_owned(),
        })
        .unwrap_or_else(|error| panic!("catalog selection failed: {error}"))
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
        decode_limits(),
        None,
    )
    .unwrap_or_else(|error| panic!("cold plan failed: {error}"))
}

fn decode_limits() -> DecodeLimits {
    DecodeLimits {
        max_stream_chunk_bytes: 8_192,
        max_packet_bytes: 2_048,
        max_packets: 8,
        max_messages: 8,
        max_decoded_event_batch_bytes: 8_192,
        max_timestamp_keys: 8,
        max_send_capture_skew_nanos: 1_000_000,
    }
}

fn fixture_plan(
    compressed_bytes: u64,
    max_pcap_bytes: u64,
    authority: &MemoryCapacityAuthority,
) -> ColdJobPlan {
    let body = catalog_body(compressed_bytes);
    let catalog = parse_catalog(&body, authority);
    plan(select_tops(&catalog), max_pcap_bytes)
}

fn catalog_body(size: u64) -> Vec<u8> {
    format!(
        "{{\"{TRADE_DATE}\":[{{\"link\":\"{DOWNLOAD_URL}\",\"date\":\"{TRADE_DATE}\",\"feed\":\"TOPS\",\"version\":\"1.6\",\"protocol\":\"IEXTP1\",\"size\":\"{size}\"}}]}}"
    )
    .into_bytes()
}

fn capture_receipt(
    plan: &ColdJobPlan,
    gzip: &[u8],
    pcap: &[u8],
    authority: &MemoryCapacityAuthority,
    clock: i64,
) -> PcapMaterializationReceipt {
    capture_receipt_with_clocks(plan, gzip, pcap, authority, clock, clock, clock)
}

fn capture_receipt_with_clocks(
    plan: &ColdJobPlan,
    gzip: &[u8],
    pcap: &[u8],
    authority: &MemoryCapacityAuthority,
    attempt_clock: i64,
    response_clock: i64,
    completed_clock: i64,
) -> PcapMaterializationReceipt {
    let permit = acquire_permit(plan, authority, attempt_clock + 30_000_000_000);
    let mut builder = GzipPcapReceiptBuilder::new(
        plan,
        permit.attempt(),
        CaptureResponseMetadata {
            response_url: DOWNLOAD_URL.to_owned(),
            status: 200,
            content_length: u64::try_from(gzip.len()).unwrap_or(u64::MAX),
            content_encoding: Some("identity".to_owned()),
            etag: Some("fixture-object".to_owned()),
            response_started_clock: trusted_clock(response_clock),
        },
    )
    .unwrap_or_else(|error| panic!("capture setup failed: {error}"));
    for chunk in gzip.chunks(17) {
        builder.push_compressed(chunk).unwrap();
    }
    for chunk in pcap.chunks(19) {
        builder.push_pcap(chunk).unwrap();
    }
    let receipt = builder.finish(trusted_clock(completed_clock), 0).unwrap();
    drop(permit);
    receipt
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

fn iex_segment(messages: &[Vec<u8>], stream_offset: i64, first_sequence: i64, send_time: i64) -> Vec<u8> {
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
    let pseudo = [10, 0, 0, 1, 233, 215, 21, 3, 0, 17, udp_length.to_be_bytes()[0], udp_length.to_be_bytes()[1]];
    let mut udp_checksum = checksum(&[&pseudo, &frame[udp_start..]]);
    if udp_checksum == 0 { udp_checksum = 0xffff; }
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

fn rewrite_second_sequence(pcap: &mut [u8], sequence: i64) {
    let first_length = usize::try_from(u32::from_le_bytes(pcap[32..36].try_into().unwrap())).unwrap();
    let second_record = 24 + 16 + first_length;
    let second_packet = second_record + 16;
    let iex = second_packet + 14 + 20 + 8;
    pcap[iex + 24..iex + 32].copy_from_slice(&sequence.to_le_bytes());
    rewrite_udp_checksum(&mut pcap[second_packet..]);
}

fn first_packet_data_offset(_: &[u8]) -> usize { 24 + 16 + 14 + 20 + 8 }

fn rewrite_udp_checksum(frame: &mut [u8]) {
    let udp_start = 14 + 20;
    frame[udp_start + 6..udp_start + 8].fill(0);
    let udp_length = [frame[udp_start + 4], frame[udp_start + 5]];
    let pseudo = [frame[26], frame[27], frame[28], frame[29], frame[30], frame[31], frame[32], frame[33], 0, 17, udp_length[0], udp_length[1]];
    let udp_len = usize::from(u16::from_be_bytes(udp_length));
    let mut value = checksum(&[&pseudo, &frame[udp_start..udp_start + udp_len]]);
    if value == 0 { value = 0xffff; }
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
    if let Some(high) = pending { sum = sum.wrapping_add(u32::from(u16::from_be_bytes([high, 0]))); }
    while sum > 0xffff { sum = (sum & 0xffff) + (sum >> 16); }
    !u16::try_from(sum).unwrap_or(0)
}
