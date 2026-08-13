//! Closed nondefault Alpaca transport fixture used by installed composition journeys.
//!
//! This module has no credential, generic URL, provider-budget mutation, account, position, order,
//! or trading surface. Its events are explicitly fixture-origin evidence.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{FutureExt as _, future::BoxFuture};
use market_squawk_domain::{AssetClass, DigestAlgorithm, EvidenceDigest, SourceId, Timestamp};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, LiveMarketSource, LiveSourceGeneration, RawMarketSink, SourceError,
    SourceMetadata, SourceMetadataProvider, TransportFrameKind,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::doctor::installed_fixture_observation;
use crate::{AlpacaError, AlpacaIexLiveConfig, AlpacaPaperIexDoctorFixtureObservation};

/// Exact source identity required by the scripted IEX live fixture.
pub const ALPACA_SCRIPTED_FIXTURE_SOURCE_ID: &str = "alpaca-installed-fixture-iex";

const DOCTOR_EVENT_COUNT: usize = 6;
const MAX_TRANSCRIPT_EVENTS: usize = 4_096;

/// Closed event kinds the Alpaca fixture can produce.
///
/// There is deliberately no account, position, order, trading, arbitrary request, or forbidden
/// route variant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaScriptedTransportEventKind {
    DoctorQuote,
    DoctorBatch,
    DoctorStreamControl,
    DoctorHistoricalPage,
    DoctorCalendar,
    DoctorCompleted,
    FixtureLiveStarted,
    FixtureLiveQuote,
}

/// One exact ordered fixture event with credential-free request and payload identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlpacaScriptedTransportEvent {
    kind: AlpacaScriptedTransportEventKind,
    request_digest: Option<EvidenceDigest>,
    payload_digests: [Option<EvidenceDigest>; 3],
}

impl AlpacaScriptedTransportEvent {
    /// Returns the closed event kind.
    pub const fn kind(&self) -> AlpacaScriptedTransportEventKind {
        self.kind
    }

    /// Returns the exact credential-free request identity when this event has one.
    pub const fn request_digest(&self) -> Option<EvidenceDigest> {
        self.request_digest
    }

    /// Returns up to three exact raw payload identities in protocol order.
    pub const fn payload_digests(&self) -> &[Option<EvidenceDigest>; 3] {
        &self.payload_digests
    }
}

/// Immutable ordered snapshot of all fixture transport events observed so far.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlpacaScriptedTransportTranscript {
    events: Box<[AlpacaScriptedTransportEvent]>,
}

impl AlpacaScriptedTransportTranscript {
    /// Returns the exact ordered event sequence.
    pub fn events(&self) -> &[AlpacaScriptedTransportEvent] {
        &self.events
    }

    /// Counts one exact closed event kind.
    pub fn count(&self, kind: AlpacaScriptedTransportEventKind) -> usize {
        self.events
            .iter()
            .filter(|event| event.kind == kind)
            .count()
    }
}

#[derive(Debug, Default)]
struct SharedTranscript {
    events: Mutex<Vec<AlpacaScriptedTransportEvent>>,
}

impl SharedTranscript {
    fn append(&self, events: &[AlpacaScriptedTransportEvent]) -> Result<(), ()> {
        let mut retained = self.events.lock().map_err(|_| ())?;
        let next_length = retained.len().checked_add(events.len()).ok_or(())?;
        if next_length > MAX_TRANSCRIPT_EVENTS {
            return Err(());
        }
        retained.try_reserve_exact(events.len()).map_err(|_| ())?;
        retained.extend_from_slice(events);
        Ok(())
    }

    fn snapshot(&self) -> Result<AlpacaScriptedTransportTranscript, ()> {
        let retained = self.events.lock().map_err(|_| ())?;
        let mut events = Vec::new();
        events.try_reserve_exact(retained.len()).map_err(|_| ())?;
        events.extend_from_slice(&retained);
        Ok(AlpacaScriptedTransportTranscript {
            events: events.into_boxed_slice(),
        })
    }
}

/// Cloneable owner of one fixed doctor and one fixed fixture-identified IEX live transport.
#[derive(Clone, Debug, Default)]
pub struct AlpacaScriptedTransportFactory {
    transcript: Arc<SharedTranscript>,
}

impl AlpacaScriptedTransportFactory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn doctor_executor(&self) -> AlpacaScriptedDoctorExecutor {
        AlpacaScriptedDoctorExecutor {
            transcript: Arc::clone(&self.transcript),
        }
    }

    /// Returns a bounded immutable snapshot of the exact ordered fixture transcript.
    pub fn transcript(&self) -> Result<AlpacaScriptedTransportTranscript, AlpacaError> {
        self.transcript
            .snapshot()
            .map_err(|()| AlpacaError::Allocation)
    }

    /// Constructs a fixture-only AAPL source without accepting credentials.
    ///
    /// The explicit source identity, metadata identity, and sole mapping must all match the closed
    /// installed-fixture contract.
    pub fn live_source(
        &self,
        fixture_source_id: SourceId,
        config: AlpacaIexLiveConfig,
        generation: LiveSourceGeneration,
    ) -> Result<AlpacaScriptedIexLiveSource, SourceError> {
        AlpacaScriptedIexLiveSource::try_new(
            fixture_source_id,
            config,
            generation,
            Arc::clone(&self.transcript),
        )
    }
}

/// Fixed five-surface doctor executor with no caller-selected request or provider-budget surface.
#[derive(Clone, Debug)]
pub struct AlpacaScriptedDoctorExecutor {
    transcript: Arc<SharedTranscript>,
}

impl AlpacaScriptedDoctorExecutor {
    pub async fn observe(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaPaperIexDoctorFixtureObservation, AlpacaError> {
        let observation = installed_fixture_observation(deadline, cancellation)?;
        let history_page = observation
            .historical()
            .pages()
            .first()
            .ok_or(AlpacaError::Protocol)?;
        let events = [
            event(
                AlpacaScriptedTransportEventKind::DoctorQuote,
                Some(observation.quote().http().request_digest()),
                [Some(observation.quote().http().body_digest()), None, None],
            ),
            event(
                AlpacaScriptedTransportEventKind::DoctorBatch,
                Some(observation.batch().http().request_digest()),
                [Some(observation.batch().http().body_digest()), None, None],
            ),
            event(
                AlpacaScriptedTransportEventKind::DoctorStreamControl,
                Some(observation.stream().request_digest()),
                [
                    Some(observation.stream().connected_frame_digest()),
                    Some(observation.stream().authenticated_frame_digest()),
                    Some(observation.stream().subscription_frame_digest()),
                ],
            ),
            event(
                AlpacaScriptedTransportEventKind::DoctorHistoricalPage,
                Some(history_page.http().request_digest()),
                [Some(history_page.http().body_digest()), None, None],
            ),
            event(
                AlpacaScriptedTransportEventKind::DoctorCalendar,
                Some(observation.calendar().http().request_digest()),
                [
                    Some(observation.calendar().http().body_digest()),
                    None,
                    None,
                ],
            ),
            event(
                AlpacaScriptedTransportEventKind::DoctorCompleted,
                None,
                [Some(observation.observation_digest()), None, None],
            ),
        ];
        debug_assert_eq!(events.len(), DOCTOR_EVENT_COUNT);
        self.transcript
            .append(&events)
            .map_err(|()| AlpacaError::Allocation)?;
        Ok(observation)
    }
}

/// Fixture-identified IEX source that emits only raw AAPL quote frames into the real sink.
#[derive(Debug)]
pub struct AlpacaScriptedIexLiveSource {
    config: AlpacaIexLiveConfig,
    authority: ActiveLiveSourceGeneration,
    transcript: Arc<SharedTranscript>,
    generation_started: bool,
}

impl AlpacaScriptedIexLiveSource {
    fn try_new(
        fixture_source_id: SourceId,
        config: AlpacaIexLiveConfig,
        generation: LiveSourceGeneration,
        transcript: Arc<SharedTranscript>,
    ) -> Result<Self, SourceError> {
        if fixture_source_id.as_str() != ALPACA_SCRIPTED_FIXTURE_SOURCE_ID
            || config.metadata().source_id() != &fixture_source_id
            || config.mappings().len() != 1
            || config.mappings()[0].symbol() != "AAPL"
            || config.mappings()[0].asset_class() != AssetClass::Equity
        {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        let authority = generation.try_start(config.metadata())?;
        Ok(Self {
            config,
            authority,
            transcript,
            generation_started: false,
        })
    }

    async fn run_scripted(
        &mut self,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        if self.generation_started {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        self.generation_started = true;
        self.authority.validate_current()?;
        let first_digest = self.publish_quote(sink)?;
        self.transcript
            .append(&[
                event(
                    AlpacaScriptedTransportEventKind::FixtureLiveStarted,
                    None,
                    [Some(fixture_identity_digest()), None, None],
                ),
                event(
                    AlpacaScriptedTransportEventKind::FixtureLiveQuote,
                    None,
                    [Some(first_digest), None, None],
                ),
            ])
            .map_err(|()| SourceError::InvalidProtocolState)?;

        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                _ = interval.tick() => {
                    let digest = self.publish_quote(sink)?;
                    self.transcript.append(&[event(
                        AlpacaScriptedTransportEventKind::FixtureLiveQuote,
                        None,
                        [Some(digest), None, None],
                    )]).map_err(|()| SourceError::InvalidProtocolState)?;
                }
            }
        }
    }

    fn publish_quote(
        &mut self,
        sink: &mut dyn RawMarketSink,
    ) -> Result<EvidenceDigest, SourceError> {
        let timestamp = system_timestamp()?;
        let payload = serde_json::to_vec(&serde_json::json!([{
            "T": "q",
            "S": "AAPL",
            "bx": "V",
            "bp": 100.00,
            "bs": 100,
            "ax": "V",
            "ap": 100.01,
            "as": 100,
            "t": rfc3339(timestamp)?
        }]))
        .map_err(|_| SourceError::InvalidProtocolState)?;
        if payload.len() > self.config.transport_limits().max_frame_bytes() {
            return Err(SourceError::FrameTooLarge {
                max: self.config.transport_limits().max_frame_bytes(),
            });
        }
        let digest = sha256(&payload);
        let frame = self
            .authority
            .frames_mut()?
            .try_frame(TransportFrameKind::Text, Bytes::from(payload))?;
        sink.try_publish(frame)?;
        Ok(digest)
    }
}

impl SourceMetadataProvider for AlpacaScriptedIexLiveSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for AlpacaScriptedIexLiveSource {
    fn run<'a>(
        &'a mut self,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        self.run_scripted(sink, cancellation).boxed()
    }
}

const fn event(
    kind: AlpacaScriptedTransportEventKind,
    request_digest: Option<EvidenceDigest>,
    payload_digests: [Option<EvidenceDigest>; 3],
) -> AlpacaScriptedTransportEvent {
    AlpacaScriptedTransportEvent {
        kind,
        request_digest,
        payload_digests,
    }
}

fn system_timestamp() -> Result<Timestamp, SourceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SourceError::TrustedTimeDiscontinuity)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_| SourceError::TrustedTimeDiscontinuity)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn rfc3339(timestamp: Timestamp) -> Result<String, SourceError> {
    Ok(
        chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(timestamp.unix_nanos())
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    )
}

fn fixture_identity_digest() -> EvidenceDigest {
    sha256(ALPACA_SCRIPTED_FIXTURE_SOURCE_ID.as_bytes())
}

fn sha256(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}
