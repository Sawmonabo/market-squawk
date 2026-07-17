#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::marker::PhantomData;
    use std::str::FromStr;
    use std::sync::{Arc, Barrier, Mutex};
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
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
        UnconfiguredAuthorizationSubjectResolver, validate_observation_profile,
    };
    use crate::policy::AuthorityStateStore;
    use crate::policy::persistence::AuthorityStateStoreError;
    use crate::{
        BudgetDecision, BudgetUnavailableReason, ChecksumValidationProfile, CurrentHealthReporter,
        CurrentSourceSession, FrameSessionBinding, LiveProtocolProfile, ProviderAggressorEvidence,
        ProviderChecksumEvidence, ProviderDecimalLexeme, ProviderNormalizedObservation,
        ProviderNumericPolicy, ProviderObservationPayload, ProviderPrice, ProviderQuantity,
        ProviderSequenceEvidence, ProviderSnapshotEvidence, ProviderTimestampEvidence,
        SemanticInterpretationProfile, SequenceValidationProfile, SessionId, SourceError,
        TransportFrameKind,
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

    #[derive(Debug, Default)]
    struct FailingAuthorityStore {
        payload: Mutex<Option<Vec<u8>>>,
        reject_stores: AtomicBool,
        store_calls: AtomicUsize,
    }

    impl FailingAuthorityStore {
        fn reject_stores(&self) {
            self.reject_stores.store(true, Ordering::Release);
        }
    }

    impl AuthorityStateStore for FailingAuthorityStore {
        fn load(&self) -> Result<Option<Vec<u8>>, AuthorityStateStoreError> {
            self.payload
                .lock()
                .map(|payload| payload.clone())
                .map_err(|_| AuthorityStateStoreError::Unavailable)
        }

        fn store(&self, payload: &[u8]) -> Result<(), AuthorityStateStoreError> {
            self.store_calls.fetch_add(1, Ordering::AcqRel);
            if self.reject_stores.load(Ordering::Acquire) {
                return Err(AuthorityStateStoreError::Unavailable);
            }
            self.payload
                .lock()
                .map_err(|_| AuthorityStateStoreError::Unavailable)?
                .replace(payload.to_vec());
            Ok(())
        }
    }

    fn durable_registry_with_test_store(
        store: Arc<dyn AuthorityStateStore>,
    ) -> Result<AuthoritativeSourceRegistry, RegistryError> {
        AuthoritativeSourceRegistry::try_new_durable_with_store_and_authorization_subject_resolver(
            store,
            Arc::new(UnconfiguredAuthorizationSubjectResolver),
        )
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
                AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_and_clock_for_diagnostics(
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
            terminal: AtomicBool::new(false),
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

    #[test]
    fn private_store_failures_publish_no_authority_and_terminalize_restrictions() -> TestResult {
        let at = Timestamp::from_unix_nanos(1_000_000_000);
        let registration_store = Arc::new(FailingAuthorityStore::default());
        let mut registration_registry =
            durable_registry_with_test_store(registration_store.clone())?;
        registration_store.reject_stores();
        assert!(matches!(
            registration_registry.register(
                direct_metadata_with_provider_and_limit(
                    "failed-registration",
                    "revision-1",
                    "failed-registration-provider",
                    10,
                )?,
                at,
            ),
            Err(RegistryError::AuthorityPersistence)
        ));

        let restrictive_store = Arc::new(FailingAuthorityStore::default());
        let mut restrictive_registry = durable_registry_with_test_store(restrictive_store.clone())?;
        let registered = restrictive_registry.register(
            direct_metadata_with_provider_and_limit(
                "failed-restriction",
                "revision-1",
                "failed-restriction-provider",
                10,
            )?,
            at,
        )?;
        let budget = registered
            .budget()
            .ok_or("restrictive budget missing")?
            .clone();
        restrictive_store.reject_stores();
        assert!(matches!(
            budget.disable(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
        ));
        assert!(matches!(
            budget.try_acquire(),
            BudgetDecision::Unavailable(BudgetUnavailableReason::PersistenceUnavailable)
        ));
        assert!(matches!(
            restrictive_registry.revoke(&registered, at),
            Err(RegistryError::AuthorityPersistence)
        ));
        assert!(matches!(
            restrictive_registry.validate_registered(&registered, at),
            Err(RegistryError::SourceRevoked)
        ));
        Ok(())
    }

    #[test]
    fn admitted_state_unwind_prevents_clean_shutdown_and_unclean_restart() -> TestResult {
        let at = Timestamp::from_unix_nanos(1_000_000_000);
        let store = Arc::new(FailingAuthorityStore::default());
        let mut registry = durable_registry_with_test_store(store.clone())?;
        let registered = registry.register(
            direct_metadata_with_provider_and_limit(
                "poisoned-shutdown",
                "revision-1",
                "poisoned-shutdown-provider",
                10,
            )?,
            at,
        )?;
        let budget = registered
            .budget()
            .ok_or("poisoned-shutdown budget missing")?;
        assert!(budget.poison_state_during_admitted_unwind_for_test());
        let calls_before_shutdown = store.store_calls.load(Ordering::Acquire);
        assert_eq!(
            registry.shutdown(),
            Err(RegistryError::ActiveAuthorityAtShutdown)
        );
        assert_eq!(
            store.store_calls.load(Ordering::Acquire),
            calls_before_shutdown,
            "shutdown published state after an admitted unwind"
        );
        assert!(matches!(
            durable_registry_with_test_store(store),
            Err(RegistryError::UncleanAuthorityPredecessor)
        ));
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
