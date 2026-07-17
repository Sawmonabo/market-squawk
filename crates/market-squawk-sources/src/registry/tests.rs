#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::marker::PhantomData;
    use std::str::FromStr;
    use std::sync::{Arc, Barrier, Mutex};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use market_squawk_domain::{
        AggressorSide, ConnectionGeneration, InstrumentId, IntegrityRule, LiveEventClass,
        MetadataRevision, RuleVersion, SequenceNumber, SequenceValidationRule, SourceId,
        SourceIdentifier, Timestamp, VenueId,
    };

    use super::{
        AuthoritativeSourceRegistry, RawFrameFactory, RegistryClock, RegistryError,
        MAX_AUTHORITY_SOURCES, SessionLeaseState, SourceAuthorityHistory, TrustedRegistryTime,
        validate_observation_profile,
    };
    use crate::{
        ChecksumValidationProfile, FrameSessionBinding, LiveProtocolProfile,
        ProviderAggressorEvidence, ProviderChecksumEvidence, ProviderDecimalLexeme,
        ProviderNormalizedObservation, ProviderNumericPolicy, ProviderObservationPayload,
        ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
        ProviderTimestampEvidence, SemanticInterpretationProfile, SequenceValidationProfile,
        CurrentHealthReporter, CurrentSourceSession, SessionId, SourceError, TransportFrameKind,
    };
    use crate::registry::test_support::{
        TestResult, direct_metadata, direct_metadata_with_provider_and_limit, healthy_snapshot,
    };

    #[derive(Clone, Copy, Debug)]
    struct ManualClockState {
        reading: TrustedRegistryTime,
        available: bool,
    }

    #[derive(Debug)]
    struct ManualRegistryClock {
        state: Mutex<ManualClockState>,
    }

    impl ManualRegistryClock {
        fn new(reading: TrustedRegistryTime) -> Self {
            Self {
                state: Mutex::new(ManualClockState {
                    reading,
                    available: true,
                }),
            }
        }

        fn set(&self, reading: TrustedRegistryTime) -> TestResult {
            let mut state = self
                .state
                .lock()
                .map_err(|_| std::io::Error::other("manual registry clock mutex poisoned"))?;
            state.reading = reading;
            state.available = true;
            Ok(())
        }

        fn fail(&self) -> TestResult {
            let mut state = self
                .state
                .lock()
                .map_err(|_| std::io::Error::other("manual registry clock mutex poisoned"))?;
            state.available = false;
            Ok(())
        }
    }

    impl RegistryClock for ManualRegistryClock {
        fn observe(&self) -> Result<TrustedRegistryTime, RegistryError> {
            let state = self
                .state
                .lock()
                .map_err(|_| RegistryError::TrustedClockUnavailable)?;
            if state.available {
                Ok(state.reading)
            } else {
                Err(RegistryError::TrustedClockUnavailable)
            }
        }

        fn shared_allocation_charge(&self) -> usize {
            std::mem::size_of::<Self>()
                + crate::conservative_arc_control_block_charge::<Self>()
        }
    }

    #[derive(Debug)]
    struct HealthHarness {
        registry: AuthoritativeSourceRegistry,
        registered: super::RegisteredSource,
        session: CurrentSourceSession,
        reporter: CurrentHealthReporter,
        clock: Arc<ManualRegistryClock>,
        wall_origin: Timestamp,
        monotonic_origin: Instant,
    }

    impl HealthHarness {
        fn new(source: &str) -> TestResult<Self> {
            let wall_origin = Timestamp::from_unix_nanos(1_000_000_000);
            let monotonic_origin = Instant::now();
            let clock = Arc::new(ManualRegistryClock::new(TrustedRegistryTime::new(
                wall_origin,
                monotonic_origin,
            )));
            let mut registry =
                AuthoritativeSourceRegistry::try_new_with_authority_state_and_clock(
                    super::RegistryAuthorityState::empty(),
                    clock.clone(),
                )?;
            let registered = registry.register(direct_metadata(source, "revision-1")?, wall_origin)?;
            let session = registry.begin_session(
                &registered,
                SessionId::new(SourceIdentifier::try_from("session-1")?),
                ConnectionGeneration::new(1)?,
                wall_origin,
            )?;
            let capabilities = registry.take_capture_generation_capabilities(&session)?;
            let (mut capture_control, _capture_admission, _capture_degradation) =
                capabilities.into_parts();
            capture_control.mark_healthy()?;
            let reporter = registry.take_current_health_reporter(&session)?;
            Ok(Self {
                registry,
                registered,
                session,
                reporter,
                clock,
                wall_origin,
                monotonic_origin,
            })
        }

        fn timestamp(&self, offset_nanos: i64) -> TestResult<Timestamp> {
            Ok(self.wall_origin.checked_add_nanos(offset_nanos)?)
        }

        fn reading(&self, wall_offset: i64, monotonic_offset: u64) -> TestResult<TrustedRegistryTime> {
            let monotonic = self
                .monotonic_origin
                .checked_add(Duration::from_nanos(monotonic_offset))
                .ok_or_else(|| std::io::Error::other("manual monotonic time overflowed"))?;
            Ok(TrustedRegistryTime::new(
                self.timestamp(wall_offset)?,
                monotonic,
            ))
        }

        fn set_time(&self, wall_offset: i64, monotonic_offset: u64) -> TestResult {
            self.clock
                .set(self.reading(wall_offset, monotonic_offset)?)
        }

        fn snapshot(&self, observed_offset: i64, deadline_offset: i64) -> TestResult<crate::SourceHealthSnapshot> {
            healthy_snapshot(
                &self.session,
                self.timestamp(observed_offset)?,
                self.timestamp(deadline_offset)?,
            )
        }

        fn accept_health(
            &mut self,
            observed_offset: i64,
            reported_offset: i64,
            accepted_offset: i64,
            deadline_offset: i64,
        ) -> TestResult {
            self.set_time(reported_offset, u64::try_from(reported_offset)?)?;
            let snapshot = self.snapshot(observed_offset, deadline_offset)?;
            let update = self.reporter.report(snapshot)?;
            self.set_time(accepted_offset, u64::try_from(accepted_offset)?)?;
            self.registry.record_health(&self.session, update)?;
            Ok(())
        }

        fn epoch_and_cursor(&self) -> (u64, i64) {
            (
                self.session.lease.health_epoch.load(std::sync::atomic::Ordering::Acquire),
                self.session
                    .lease
                    .last_health_observed_nanos
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        }
    }


    #[test]
    fn frame_ordinal_exhaustion_terminally_invalidates_factory()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = FrameSessionBinding::new(
            SourceId::try_from("source-a")?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
            SessionId::new(SourceIdentifier::try_from("session-a")?),
            ConnectionGeneration::new(1)?,
        );
        let lease = Arc::new(SessionLeaseState {
            current: AtomicBool::new(true),
            live_qualified: AtomicBool::new(false),
            health_epoch: AtomicU64::new(0),
            valid_from_nanos: AtomicI64::new(i64::MAX),
            valid_until_nanos: AtomicI64::new(i64::MIN),
            last_health_observed_nanos: AtomicI64::new(i64::MIN),
            frame_ordinal: AtomicU64::new(u64::MAX),
        });
        let mut factory = RawFrameFactory {
            binding,
            lease: Arc::clone(&lease),
            not_sync: PhantomData::<Cell<()>>,
        };
        assert!(matches!(
            factory.try_frame(
                Timestamp::from_unix_nanos(1),
                TransportFrameKind::Binary,
                Bytes::from_static(b"frame"),
            ),
            Err(SourceError::FrameIdentityExhausted)
        ));
        assert!(!lease.is_current());
        Ok(())
    }

    #[test]
    fn semantic_rules_cannot_be_transplanted_across_event_families()
    -> Result<(), Box<dyn std::error::Error>> {
        let aggressor = rule("aggressor")?;
        let corporate_action = rule("corporate-action")?;
        let timestamp = rule("timestamp")?;
        let sequence = rule("sequence")?;
        let no_checksum = rule("no-checksum")?;
        let no_snapshot = rule("no-snapshot")?;
        let protocol = LiveProtocolProfile::new(
            rule("decoder")?,
            SemanticInterpretationProfile::new(
                aggressor.clone(),
                rule("auction")?,
                rule("trading-status")?,
                corporate_action.clone(),
            ),
            timestamp.clone(),
            SequenceValidationProfile::Provided {
                rule: sequence.clone(),
                progression: SequenceValidationRule::Consecutive,
            },
            ChecksumValidationProfile::Unsupported {
                rule: no_checksum.clone(),
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        );
        let observation = |payload| {
            ProviderNormalizedObservation::try_new(
                SourceIdentifier::try_from("message-1")?,
                VenueId::try_from("coinbase")?,
                InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
                ProviderTimestampEvidence::Provided {
                    value: Timestamp::from_unix_nanos(1),
                    rule: timestamp.clone(),
                },
                ProviderSequenceEvidence::Provided {
                    value: SequenceNumber::new(1),
                    rule: sequence.clone(),
                },
                ProviderSnapshotEvidence::NotApplicable(no_snapshot.clone()),
                ProviderChecksumEvidence::Unsupported {
                    rule: no_checksum.clone(),
                },
                payload,
            )
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let price = || {
            ProviderDecimalLexeme::try_new("1")
                .map(ProviderPrice::new)
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let quantity = || {
            ProviderDecimalLexeme::try_new("1")
                .map(ProviderQuantity::new)
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let valid = observation(ProviderObservationPayload::Trade {
            trade_id: SourceIdentifier::try_from("trade-1")?,
            price: price()?,
            quantity: quantity()?,
            aggressor: ProviderAggressorEvidence::new(AggressorSide::Buy, None, aggressor),
        })?;
        assert!(validate_observation_profile(&protocol, &valid).is_ok());

        let transplanted = observation(ProviderObservationPayload::Trade {
            trade_id: SourceIdentifier::try_from("trade-2")?,
            price: price()?,
            quantity: quantity()?,
            aggressor: ProviderAggressorEvidence::new(AggressorSide::Buy, None, corporate_action),
        })?;
        assert!(validate_observation_profile(&protocol, &transplanted).is_err());
        Ok(())
    }

    include!("tests/temporal_cases.rs");

    fn rule(value: &str) -> Result<IntegrityRule, Box<dyn std::error::Error>> {
        Ok(IntegrityRule::new(
            SourceIdentifier::try_from(value)?,
            RuleVersion::new(1)?,
        ))
    }
}
