mod metadata;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, bail};
use market_squawk_domain::{
    AggressorSide, CaptureIntegrityState, CaptureResidentGenerationLease, CaptureResidentToken,
    ConnectionGeneration, MarketDepth, ProviderChannel, ProviderProduct, SequenceNumber,
    StreamIntegrityState, Timestamp, VenueId,
};
use market_squawk_live::{BoundShardIngress, ShardKey};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationHealth, BudgetHealth, CaptureAdmissionIssuer,
    CaptureDegradationCapability, ConnectionLiveness, CoverageHealth, CurrentDecodedProviderBatch,
    CurrentHealthReporter, CurrentSourceSession, DecodeOutcome, DecodedProviderBatch,
    DecoderEvidence, ProviderAggressorEvidence, ProviderBookChange, ProviderBookLevel,
    ProviderBookSide, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderPrice, ProviderQuantity,
    ProviderSequenceEvidence, ProviderSnapshotEvidence, ProviderTimestampEvidence, RawFrameFactory,
    RegisteredSource, SessionId, SourceHealthSnapshot, TransportFrameKind,
    ValidatedSessionDecodeOutcome,
};
use tokio_util::sync::CancellationToken;

use super::ReleaseBenchmarkObserver;
use crate::LiveRuntimeComposition;

const BATCH_SIZE: usize = 32;

pub(super) fn instrument_definition() -> Result<market_squawk_domain::InstrumentDefinition> {
    metadata::instrument_definition()
}

#[derive(Debug)]
struct BenchmarkResidentToken;

impl CaptureResidentToken for BenchmarkResidentToken {}

#[derive(Debug)]
pub(super) struct ReleaseBenchmarkSource {
    _registry: AuthoritativeSourceRegistry,
    _registered: RegisteredSource,
    session: CurrentSourceSession,
    capture_admission: CaptureAdmissionIssuer,
    _capture_degradation: CaptureDegradationCapability,
    frames: RawFrameFactory,
    _reporter: CurrentHealthReporter,
    ingress: BoundShardIngress,
    next_sequence: u64,
}

impl ReleaseBenchmarkSource {
    pub(super) async fn start(
        live: &LiveRuntimeComposition,
        route: ShardKey,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        let at = now()?;
        let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
        let registered = registry.register(metadata::source_metadata()?, at)?;
        let session = registry.begin_session(
            &registered,
            SessionId::new(metadata::identifier("release-performance-session")?),
            ConnectionGeneration::new(1)?,
            at,
        )?;
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let (mut capture_control, capture_admission, capture_degradation) =
            capabilities.into_parts();
        capture_control.mark_healthy()?;
        let frames = registry.take_raw_frame_factory(&session)?;
        let mut reporter = registry.take_current_health_reporter(&session)?;
        let valid_until = at.checked_add_nanos(i64::try_from(metadata::FRESHNESS_NANOS)?)?;
        let health = SourceHealthSnapshot::try_new(
            &session,
            at,
            ConnectionLiveness::Live {
                last_activity_at: at,
            },
            Some(at),
            Some(at),
            Some(at),
            metadata::freshness()?,
            StreamIntegrityState::Healthy,
            CaptureIntegrityState::Healthy,
            AuthorizationHealth::Valid {
                evidence: metadata::evidence(11),
                valid_until,
            },
            CoverageHealth::Sufficient {
                evidence: metadata::evidence(12),
                provider_product: ProviderProduct::new(metadata::identifier(
                    "release-performance-diagnostic",
                )?),
                provider_channel: ProviderChannel::new(metadata::identifier(
                    "bounded-local-ingress",
                )?),
                valid_until,
            },
            BudgetHealth::Available,
            None,
            Vec::new(),
        )?;
        let update = reporter.report(health)?;
        registry.record_health(&session, update)?;
        let lease = registry
            .validate_current_authority(&session)?
            .try_current_lease()?;
        let ingress = live.bind_generation(route, lease, cancellation).await?;
        Ok(Self {
            _registry: registry,
            _registered: registered,
            session,
            capture_admission,
            _capture_degradation: capture_degradation,
            frames,
            _reporter: reporter,
            ingress,
            next_sequence: 1,
        })
    }

    pub(super) async fn initialize(&mut self, observer: &ReleaseBenchmarkObserver) -> Result<()> {
        let batch = self.current_batch(BatchKind::Snapshot)?;
        self.publish(batch, 1, observer).await
    }

    pub(super) async fn publish_trades(
        &mut self,
        mut event_count: u64,
        observer: &ReleaseBenchmarkObserver,
    ) -> Result<()> {
        while event_count != 0 {
            let count = usize::try_from(event_count.min(BATCH_SIZE as u64))
                .context("benchmark batch count exceeds usize")?;
            let batch = self.current_batch(BatchKind::Trades(count))?;
            self.publish(batch, u64::try_from(count)?, observer).await?;
            event_count = event_count
                .checked_sub(u64::try_from(count)?)
                .context("benchmark event count underflow")?;
        }
        Ok(())
    }

    pub(super) async fn publish_dispatch_delta(
        &mut self,
        observer: &ReleaseBenchmarkObserver,
    ) -> Result<()> {
        let batch = self.current_batch(BatchKind::DispatchDelta)?;
        self.publish(batch, 1, observer).await
    }

    async fn publish(
        &mut self,
        batch: CurrentDecodedProviderBatch,
        events: u64,
        observer: &ReleaseBenchmarkObserver,
    ) -> Result<()> {
        let target = observer.begin_batch(events)?;
        self.ingress
            .try_publish(batch)
            .context("diagnostic benchmark ingress refused a capacity-permitted batch")?;
        observer.wait_for(target, Duration::from_secs(5)).await
    }

    fn current_batch(&mut self, kind: BatchKind) -> Result<CurrentDecodedProviderBatch> {
        let frame = self
            .frames
            .try_frame(TransportFrameKind::Binary, vec![1_u8].into())?;
        self.capture_admission.preflight(&frame)?;
        let receipt = self.capture_admission.issue_after_enqueue(
            &frame,
            CaptureResidentGenerationLease::new(Arc::new(BenchmarkResidentToken)),
        )?;
        self.capture_admission.validate_active(&frame)?;
        let validated_frame = self.session.validate_live_frame(&frame)?;
        let decoder = DecoderEvidence::from_validated_frame(
            &validated_frame,
            metadata::rule("release-benchmark-decoder")?,
        );
        let observations = self.observations(kind, frame.received_at())?;
        let decoded = DecodedProviderBatch::try_new(decoder, observations)?;
        let validated_session = self
            ._registry
            .validate_session(&self.session, frame.received_at())?;
        let outcome = validated_session
            .validate_decode_outcome_owned(DecodeOutcome::Data(decoded), receipt)?;
        let ValidatedSessionDecodeOutcome::Data(captured) = outcome else {
            bail!("diagnostic benchmark data changed decode disposition");
        };
        let current = self._registry.validate_current_authority(&self.session)?;
        let batches = current.validate_data_outcome_owned(captured)?;
        let mut batches = batches.into_iter();
        let batch = batches
            .next()
            .context("diagnostic benchmark produced no current route batch")?;
        if batches.next().is_some() {
            bail!("diagnostic benchmark produced more than one route batch");
        }
        Ok(batch)
    }

    fn observations(
        &mut self,
        kind: BatchKind,
        observed_at: Timestamp,
    ) -> Result<Vec<ProviderNormalizedObservation>> {
        let count = match kind {
            BatchKind::Snapshot | BatchKind::DispatchDelta => 1,
            BatchKind::Trades(count) => count,
        };
        let mut observations = Vec::new();
        observations.try_reserve_exact(count)?;
        for _ in 0..count {
            let sequence = self.take_sequence()?;
            observations.push(self.observation(kind, sequence, observed_at)?);
        }
        Ok(observations)
    }

    fn observation(
        &self,
        kind: BatchKind,
        sequence: u64,
        observed_at: Timestamp,
    ) -> Result<ProviderNormalizedObservation> {
        let source_identifier = metadata::identifier(format!("release-event-{sequence}"))?;
        let payload = match kind {
            BatchKind::Snapshot => ProviderObservationPayload::book_snapshot(
                MarketDepth::PriceLevel,
                vec![level("100.00", "1.00")?],
                vec![level("101.00", "1.00")?],
            )?,
            BatchKind::Trades(_) => ProviderObservationPayload::Trade {
                trade_id: source_identifier.clone(),
                price: price("100.50")?,
                quantity: quantity("1.00")?,
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Buy,
                    Some(metadata::identifier("BUY")?),
                    metadata::rule("release-benchmark-aggressor")?,
                ),
            },
            BatchKind::DispatchDelta => ProviderObservationPayload::book_delta(
                MarketDepth::PriceLevel,
                vec![
                    ProviderBookChange::new(ProviderBookSide::Bid, level("100.00", "2.00")?),
                    ProviderBookChange::new(ProviderBookSide::Ask, level("101.00", "1.00")?),
                ],
            )?,
        };
        let snapshot = match kind {
            BatchKind::Snapshot => ProviderSnapshotEvidence::InitializingSnapshot {
                provider_reference: Some(source_identifier.clone()),
            },
            BatchKind::DispatchDelta => ProviderSnapshotEvidence::Delta {
                provider_snapshot_reference: None,
            },
            BatchKind::Trades(_) => ProviderSnapshotEvidence::NotApplicable(metadata::rule(
                "release-benchmark-non-book",
            )?),
        };
        Ok(ProviderNormalizedObservation::try_new(
            source_identifier,
            VenueId::try_from(metadata::VENUE_ID)?,
            metadata::INSTRUMENT_ID.parse()?,
            ProviderTimestampEvidence::Provided {
                value: observed_at,
                rule: metadata::rule("release-benchmark-timestamp")?,
            },
            ProviderSequenceEvidence::Provided {
                value: SequenceNumber::new(sequence),
                rule: metadata::rule("release-benchmark-sequence")?,
            },
            snapshot,
            ProviderChecksumEvidence::Unsupported {
                rule: metadata::rule("release-benchmark-no-checksum")?,
            },
            payload,
        )?)
    }

    fn take_sequence(&mut self) -> Result<u64> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .context("diagnostic benchmark sequence exhausted")?;
        Ok(sequence)
    }
}

#[derive(Clone, Copy)]
enum BatchKind {
    Snapshot,
    Trades(usize),
    DispatchDelta,
}

fn level(price_value: &str, quantity_value: &str) -> Result<ProviderBookLevel> {
    Ok(ProviderBookLevel::new(
        price(price_value)?,
        quantity(quantity_value)?,
    ))
}

fn price(value: &str) -> Result<ProviderPrice> {
    Ok(ProviderPrice::new(ProviderDecimalLexeme::try_new(value)?))
}

fn quantity(value: &str) -> Result<ProviderQuantity> {
    Ok(ProviderQuantity::new(ProviderDecimalLexeme::try_new(
        value,
    )?))
}

fn now() -> Result<Timestamp> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(Timestamp::from_unix_nanos(i64::try_from(
        elapsed.as_nanos(),
    )?))
}
