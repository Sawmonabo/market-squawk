//! Deterministic production message and identity fixtures.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureAuthorityIdentity, ConnectionGeneration, MetadataRevision,
    RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
};

use super::super::{
    CaptureChannelLimits, CaptureDestination, CaptureMessage, CaptureProcessInfrastructure,
    CaptureProcessInfrastructureLimits, CaptureState, DiagnosticCaptureBundle,
    DiagnosticCaptureFrame, RecordReservationQuote, initialize_capture_process_infrastructure,
};
use super::types::BenchmarkSupportError;

static DESTINATION_ORDINAL: AtomicU64 = AtomicU64::new(0);
pub(super) const BENCHMARK_RECORD_RESERVATION_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const BENCHMARK_CAPTURE_MEMORY_CEILING_BYTES: usize = 256 * 1024 * 1024;
const BENCHMARK_DESTINATION_REGISTRY_CEILING_BYTES: usize = 1024 * 1024;

pub(super) fn process_infrastructure() -> Result<CaptureProcessInfrastructure, BenchmarkSupportError>
{
    initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
        NonZeroUsize::new(BENCHMARK_DESTINATION_REGISTRY_CEILING_BYTES)
            .unwrap_or(NonZeroUsize::MIN),
    ))
    .map_err(|_error| BenchmarkSupportError::CaptureComposition)
}

pub(super) fn channel_limits(queue_depth: NonZeroUsize) -> CaptureChannelLimits {
    CaptureChannelLimits::new(
        queue_depth,
        NonZeroUsize::new(BENCHMARK_CAPTURE_MEMORY_CEILING_BYTES).unwrap_or(NonZeroUsize::MIN),
    )
}

#[derive(Debug)]
pub(super) struct PreparedFixture {
    pub(super) frame: DiagnosticCaptureFrame,
    pub(super) effective_capacity: NonZeroUsize,
}

#[derive(Debug)]
pub(super) struct MessageFactory {
    capture_state: Arc<CaptureState<DiagnosticCaptureBundle>>,
    frame: DiagnosticCaptureFrame,
    reservation_bytes: usize,
}

pub(super) fn prepare_fixture(
    payload_bytes: usize,
    queue_depth: NonZeroUsize,
) -> Result<PreparedFixture, BenchmarkSupportError> {
    let frame = fixture_frame(payload_bytes)?;
    let effective_capacity = effective_capacity::<DiagnosticCaptureBundle>(&frame, queue_depth)?;
    Ok(PreparedFixture {
        frame,
        effective_capacity,
    })
}

impl MessageFactory {
    pub(super) fn try_new(
        capture_state: Arc<CaptureState<DiagnosticCaptureBundle>>,
        frame: DiagnosticCaptureFrame,
    ) -> Result<Self, BenchmarkSupportError> {
        let reservation_bytes = reservation_bytes::<DiagnosticCaptureBundle>(&frame)?;
        Ok(Self {
            capture_state,
            frame,
            reservation_bytes,
        })
    }

    pub(super) fn prepare(
        &self,
    ) -> Result<CaptureMessage<DiagnosticCaptureBundle>, BenchmarkSupportError> {
        let active = self.capture_state.active.load_full();
        let reservation = self
            .capture_state
            .try_reserve_queue_bytes(self.reservation_bytes)
            .map_err(|_error| BenchmarkSupportError::ObservationInvariant)?;
        Ok(CaptureMessage::Record {
            allocation: active,
            frame: Arc::new(self.frame.clone()),
            reservation,
        })
    }
}

pub(super) fn fixture_identity() -> Result<CaptureAuthorityIdentity, BenchmarkSupportError> {
    Ok(CaptureAuthorityIdentity::new(
        SourceId::try_from("capture-benchmark")
            .map_err(|_error| BenchmarkSupportError::InvalidFixture)?,
        MetadataRevision::new(
            SourceIdentifier::try_from("fixture-v1")
                .map_err(|_error| BenchmarkSupportError::InvalidFixture)?,
        ),
        SourceIdentifier::try_from("session-v1")
            .map_err(|_error| BenchmarkSupportError::InvalidFixture)?,
        ConnectionGeneration::new(1).map_err(|_error| BenchmarkSupportError::InvalidFixture)?,
    ))
}

pub(super) fn next_destination() -> Result<CaptureDestination, BenchmarkSupportError> {
    let ordinal = DESTINATION_ORDINAL
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    CaptureDestination::try_named(&format!("capture-benchmark-{ordinal}"))
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)
}

fn fixture_frame(payload_bytes: usize) -> Result<DiagnosticCaptureFrame, BenchmarkSupportError> {
    DiagnosticCaptureFrame::try_new(
        fixture_identity()?,
        NonZeroU64::MIN,
        Timestamp::from_unix_nanos(1),
        Bytes::from(vec![0_u8; payload_bytes]),
    )
    .map_err(|_error| BenchmarkSupportError::InvalidFixture)
}

fn effective_capacity<B: CaptureAuthorityBundle>(
    frame: &B::Frame,
    queue_depth: NonZeroUsize,
) -> Result<NonZeroUsize, BenchmarkSupportError> {
    let reservation_bytes = reservation_bytes::<B>(frame)?;
    NonZeroUsize::new(
        queue_depth
            .get()
            .min(BENCHMARK_RECORD_RESERVATION_BUDGET_BYTES / reservation_bytes),
    )
    .ok_or(BenchmarkSupportError::InvalidFixture)
}

fn reservation_bytes<B: CaptureAuthorityBundle>(
    frame: &B::Frame,
) -> Result<usize, BenchmarkSupportError> {
    let complete = frame
        .checked_retained_footprint()
        .and_then(|footprint| footprint.checked_complete_bytes())
        .map_err(|_error| BenchmarkSupportError::InvalidFixture)?;
    let bytes = RecordReservationQuote::try_for_frame::<B>(frame, complete)
        .and_then(RecordReservationQuote::checked_total)
        .map_err(|_error| BenchmarkSupportError::InvalidFixture)?;
    if bytes == 0 {
        return Err(BenchmarkSupportError::InvalidFixture);
    }
    Ok(bytes)
}

#[cfg(test)]
pub(super) fn reservation_bytes_for_test(
    payload_bytes: usize,
) -> Result<usize, BenchmarkSupportError> {
    reservation_bytes::<DiagnosticCaptureBundle>(&fixture_frame(payload_bytes)?)
}
