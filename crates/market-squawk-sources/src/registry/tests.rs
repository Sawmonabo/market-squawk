#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::marker::PhantomData;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use market_squawk_domain::{
        AggressorSide, ConnectionGeneration, DataQuality, InstrumentId, IntegrityRule,
        LiveEventClass, MarketDepth, MetadataRevision, RuleVersion, SequenceNumber,
        SequenceValidationRule, SourceId, SourceIdentifier, Timestamp, VenueId,
    };

    use super::{
        AuthoritativeSourceRegistry, BoundedVec, MAX_AUTHORITY_SOURCES, PersistedSourceAuthority,
        RawFrameFactory, RegistryAuthorityState, RegistryError, SessionLeaseState,
        SourceAuthorityHistory, UnconfiguredAuthorizationSubjectResolver,
        validate_observation_profile,
    };
    use crate::authority_time::{
        RawRegistryClockObservation, RawRegistryClockSource, RegistryMonotonicInstant,
        SealedRegistryClock, TrustedRegistryTime,
    };
    use crate::policy::AuthorityStateStore;
    use crate::policy::persistence::AuthorityStateStoreError;
    use crate::registry::test_support::{
        TestResult, direct_metadata, direct_metadata_with_provider_and_limit,
        direct_metadata_with_quality, direct_metadata_with_revision_evidence, exact_evidence,
        extraction_metadata, freshness_policy, healthy_snapshot, source_identifier,
    };
    use crate::{
        BudgetDecision, BudgetUnavailableReason, ChecksumValidationProfile, CurrentHealthReporter,
        CurrentSourceSession, FrameSessionBinding, LiveProtocolProfile, ProviderAggressorEvidence,
        ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderChecksumEvidence,
        ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderNumericPolicy,
        ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
        ProviderSnapshotEvidence, ProviderTimestampEvidence, SemanticInterpretationProfile,
        SequenceValidationProfile, SessionId, SourceError, TransportFrameKind,
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

    fn durable_registry_with_test_store_and_clock(
        store: Arc<dyn AuthorityStateStore>,
        clock: Arc<dyn RawRegistryClockSource>,
    ) -> Result<AuthoritativeSourceRegistry, RegistryError> {
        AuthoritativeSourceRegistry::try_new_durable_with_store_resolver_and_clock_source(
            store,
            Arc::new(UnconfiguredAuthorizationSubjectResolver),
            clock,
        )
    }

    fn durable_registry_with_test_store_for_exclusive_installed_replacement(
        store: Arc<dyn AuthorityStateStore>,
    ) -> Result<AuthoritativeSourceRegistry, RegistryError> {
        AuthoritativeSourceRegistry::try_new_durable_with_store_for_exclusive_installed_replacement_for_test(
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

    impl RawRegistryClockSource for ManualRegistryClock {
        fn observe_raw(&self) -> Result<RawRegistryClockObservation, RegistryError> {
            let state = self
                .state
                .lock()
                .map_err(|_| RegistryError::TrustedClockUnavailable)?;
            if state.available {
                Ok(RawRegistryClockObservation::new(
                    state.reading.wall(),
                    state.reading.monotonic(),
                ))
            } else {
                Err(RegistryError::TrustedClockUnavailable)
            }
        }

        fn shared_allocation_charge(&self) -> usize {
            std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
        }
    }

    #[test]
    fn active_live_generation_validates_only_its_exact_current_frames() -> TestResult {
        let metadata = direct_metadata("active-frame-source", "active-frame-revision")?;
        let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
        let registered = registry.register(metadata.clone(), Timestamp::from_unix_nanos(1))?;
        let session = registry.begin_session(
            &registered,
            SessionId::new(SourceIdentifier::try_from("active-frame-session")?),
            ConnectionGeneration::new(1)?,
            Timestamp::from_unix_nanos(1),
        )?;
        let capture = registry.take_capture_generation_capabilities(&session)?;
        let (mut initialization, _admission, _degradation) = capture.into_parts();
        initialization.mark_healthy()?;
        let generation = registry.take_live_source_generation(&session)?;
        let mut active = generation.try_start(&metadata)?;

        assert_eq!(active.generation(), ConnectionGeneration::new(1)?);
        let frame = active.frames_mut()?.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(br#"{"type":"fixture"}"#),
        )?;
        let validated = active.validate_live_frame(&frame)?;
        assert_eq!(validated.frame().frame_id(), frame.frame_id());

        let _successor = registry.begin_session(
            &registered,
            SessionId::new(SourceIdentifier::try_from("active-frame-successor")?),
            ConnectionGeneration::new(2)?,
            Timestamp::from_unix_nanos(2),
        )?;
        assert!(matches!(
            active.validate_live_frame(&frame),
            Err(SourceError::SessionNotCurrent)
        ));
        Ok(())
    }

    #[derive(Debug)]
    struct HealthHarness {
        registry: AuthoritativeSourceRegistry,
        registered: super::RegisteredSource,
        session: CurrentSourceSession,
        reporter: CurrentHealthReporter,
        clock: Arc<ManualRegistryClock>,
        wall_origin: Timestamp,
        monotonic_origin: RegistryMonotonicInstant,
    }

    impl HealthHarness {
        fn new(source: &str) -> TestResult<Self> {
            Self::new_with_quality(source, DataQuality::DirectVerified)
        }

        fn new_with_quality(source: &str, quality_ceiling: DataQuality) -> TestResult<Self> {
            let wall_origin = Timestamp::from_unix_nanos(1_000_000_000);
            let monotonic_origin = RegistryMonotonicInstant::from_nanos(0);
            let clock = Arc::new(ManualRegistryClock::new(TrustedRegistryTime::new(
                wall_origin,
                monotonic_origin,
            )));
            let mut registry =
                AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_and_clock_for_diagnostics(
                    super::RegistryAuthorityState::empty(),
                    clock.clone(),
                )?;
            let registered = registry.register(
                direct_metadata_with_quality(source, "revision-1", quality_ceiling)?,
                wall_origin,
            )?;
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

        fn reading(
            &self,
            wall_offset: i64,
            monotonic_offset: u64,
        ) -> TestResult<TrustedRegistryTime> {
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
            self.clock.set(self.reading(wall_offset, monotonic_offset)?)
        }

        fn snapshot(
            &self,
            observed_offset: i64,
            deadline_offset: i64,
        ) -> TestResult<crate::SourceHealthSnapshot> {
            healthy_snapshot(
                &self.session,
                self.timestamp(observed_offset)?,
                self.timestamp(deadline_offset)?,
            )
        }

        fn snapshot_with_source_timestamp(
            &self,
            observed_offset: i64,
            source_offset: Option<i64>,
            deadline_offset: i64,
        ) -> TestResult<crate::SourceHealthSnapshot> {
            let observed_at = self.timestamp(observed_offset)?;
            let source_at = source_offset
                .map(|offset| self.timestamp(offset))
                .transpose()?;
            let valid_until = self.timestamp(deadline_offset)?;
            Ok(crate::SourceHealthSnapshot::try_new(
                &self.session,
                observed_at,
                crate::ConnectionLiveness::Live {
                    last_activity_at: observed_at,
                },
                Some(observed_at),
                Some(observed_at),
                source_at,
                freshness_policy()?,
                market_squawk_domain::StreamIntegrityState::Healthy,
                market_squawk_domain::CaptureIntegrityState::Healthy,
                crate::AuthorizationHealth::Valid {
                    evidence: exact_evidence(31),
                    valid_until,
                },
                crate::CoverageHealth::Sufficient {
                    evidence: exact_evidence(32),
                    provider_product: market_squawk_domain::ProviderProduct::new(
                        source_identifier("direct-product")?,
                    ),
                    provider_channel: market_squawk_domain::ProviderChannel::new(
                        source_identifier("trades")?,
                    ),
                    valid_until,
                },
                crate::BudgetHealth::Available,
                None,
                Vec::new(),
            )?)
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
                self.session
                    .lease
                    .health_epoch
                    .load(std::sync::atomic::Ordering::Acquire),
                self.session
                    .lease
                    .last_health_observed_nanos
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        }
    }

    #[test]
    fn source_timestamp_freshness_is_quality_ceiling_aware() -> TestResult {
        let mut research = HealthHarness::new_with_quality(
            "research-current-data",
            DataQuality::DirectUnverified,
        )?;
        research.set_time(20, 20)?;
        let uninitialized = research.snapshot_with_source_timestamp(10, None, 1_000)?;
        let update = research.reporter.report(uninitialized)?;
        research.set_time(30, 30)?;
        research.registry.record_health(&research.session, update)?;
        research
            .registry
            .validate_current_authority(&research.session)?;

        let mut executable = HealthHarness::new("executable-current-data")?;
        executable.set_time(20, 20)?;
        let uninitialized = executable.snapshot_with_source_timestamp(10, None, 1_000)?;
        let update = executable.reporter.report(uninitialized)?;
        executable.set_time(30, 30)?;
        executable
            .registry
            .record_health(&executable.session, update)?;
        assert_eq!(
            executable
                .registry
                .validate_current_authority(&executable.session)
                .map(|_| ()),
            Err(RegistryError::HealthNotQualified)
        );

        research.set_time(40, 40)?;
        let stale = research.snapshot_with_source_timestamp(31, Some(-2_000_000_000), 1_000)?;
        let update = research.reporter.report(stale)?;
        research.set_time(50, 50)?;
        research.registry.record_health(&research.session, update)?;
        assert_eq!(
            research
                .registry
                .validate_current_authority(&research.session)
                .map(|_| ()),
            Err(RegistryError::HealthNotQualified)
        );
        Ok(())
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
        let raw_clock = Arc::new(ManualRegistryClock::new(TrustedRegistryTime::new(
            Timestamp::from_unix_nanos(1),
            RegistryMonotonicInstant::from_nanos(1),
        )));
        let clock = Arc::new(SealedRegistryClock::new(raw_clock));
        let started_at = clock.observe()?;
        let lease = Arc::new(SessionLeaseState {
            current: AtomicBool::new(true),
            terminal: AtomicBool::new(false),
            live_qualified: AtomicBool::new(false),
            health_epoch: AtomicU64::new(0),
            minimum_valid_health_epoch: AtomicU64::new(0),
            valid_from_nanos: AtomicI64::new(i64::MAX),
            valid_until_nanos: AtomicI64::new(i64::MIN),
            last_health_observed_nanos: AtomicI64::new(i64::MIN),
            frame_ordinal: AtomicU64::new(u64::MAX),
            continuity: clock.continuity().clone(),
            started_at,
        });
        let mut factory = RawFrameFactory {
            binding,
            lease: Arc::clone(&lease),
            clock,
            not_sync: PhantomData::<Cell<()>>,
        };
        assert!(matches!(
            factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"frame"),),
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
            taker_order_type: None,
        })?;
        assert!(
            validate_observation_profile(&protocol, DataQuality::DirectVerified, &valid).is_ok()
        );

        let transplanted = observation(ProviderObservationPayload::Trade {
            trade_id: SourceIdentifier::try_from("trade-2")?,
            price: price()?,
            quantity: quantity()?,
            aggressor: ProviderAggressorEvidence::new(AggressorSide::Buy, None, corporate_action),
            taker_order_type: None,
        })?;
        assert!(
            validate_observation_profile(&protocol, DataQuality::DirectVerified, &transplanted)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn absent_timestamp_only_initializes_non_executable_snapshot_state() -> TestResult {
        let timestamp = rule("mixed-timestamp")?;
        let no_sequence = rule("no-sequence")?;
        let no_checksum = rule("no-checksum")?;
        let no_snapshot = rule("no-snapshot")?;
        let protocol = LiveProtocolProfile::new(
            rule("decoder")?,
            SemanticInterpretationProfile::new(
                rule("aggressor")?,
                rule("auction")?,
                rule("trading-status")?,
                rule("corporate-action")?,
            ),
            timestamp.clone(),
            SequenceValidationProfile::Unsupported {
                rule: no_sequence.clone(),
            },
            ChecksumValidationProfile::Unsupported {
                rule: no_checksum.clone(),
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        );
        let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
        let observation = |id, snapshot, payload| {
            ProviderNormalizedObservation::try_new(
                SourceIdentifier::try_from(id)?,
                VenueId::try_from("coinbase")?,
                instrument,
                ProviderTimestampEvidence::AuthoritativelyAbsent(timestamp.clone()),
                ProviderSequenceEvidence::Unsupported {
                    rule: no_sequence.clone(),
                },
                snapshot,
                ProviderChecksumEvidence::Unsupported {
                    rule: no_checksum.clone(),
                },
                payload,
            )
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
        };
        let initializing = observation(
            "snapshot",
            ProviderSnapshotEvidence::InitializingSnapshot {
                provider_reference: None,
            },
            ProviderObservationPayload::book_snapshot(
                MarketDepth::PriceLevel,
                Vec::new(),
                Vec::new(),
            )?,
        )?;
        assert!(
            validate_observation_profile(&protocol, DataQuality::DirectUnverified, &initializing,)
                .is_ok()
        );
        assert_eq!(
            validate_observation_profile(&protocol, DataQuality::DirectVerified, &initializing),
            Err(RegistryError::DecoderProfileMismatch)
        );

        let level = ProviderBookLevel::new(
            ProviderPrice::new(ProviderDecimalLexeme::try_new("1")?),
            ProviderQuantity::new(ProviderDecimalLexeme::try_new("1")?),
        );
        let delta = observation(
            "delta",
            ProviderSnapshotEvidence::Delta {
                provider_snapshot_reference: None,
            },
            ProviderObservationPayload::book_delta(
                MarketDepth::PriceLevel,
                vec![ProviderBookChange::new(ProviderBookSide::Bid, level)],
            )?,
        )?;
        assert_eq!(
            validate_observation_profile(&protocol, DataQuality::DirectUnverified, &delta),
            Err(RegistryError::DecoderProfileMismatch)
        );

        let trade = observation(
            "trade",
            ProviderSnapshotEvidence::NotApplicable(no_snapshot),
            ProviderObservationPayload::Trade {
                trade_id: SourceIdentifier::try_from("trade")?,
                price: ProviderPrice::new(ProviderDecimalLexeme::try_new("1")?),
                quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1")?),
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Buy,
                    None,
                    protocol.semantic_interpretation().aggressor_rule().clone(),
                ),
                taker_order_type: None,
            },
        )?;
        assert_eq!(
            validate_observation_profile(&protocol, DataQuality::DirectUnverified, &trade),
            Err(RegistryError::DecoderProfileMismatch)
        );
        Ok(())
    }

    #[test]
    fn exact_restart_resume_preserves_revision_and_allocates_the_next_generation() -> TestResult {
        let at = Timestamp::from_unix_nanos(1_000_000_000);
        let store = Arc::new(FailingAuthorityStore::default());
        let metadata = direct_metadata("restart-resume", "revision-1")?;
        let mut first = durable_registry_with_test_store(store.clone())?;
        let registered = first.register_or_resume_exact(metadata.clone(), at)?;
        let session = first.begin_session(
            &registered,
            SessionId::new(SourceIdentifier::try_from("session-7")?),
            ConnectionGeneration::new(7)?,
            at,
        )?;
        first.end_session(&session, at)?;
        drop(session);
        drop(registered);
        first.shutdown()?;

        let mut restarted = durable_registry_with_test_store(store.clone())?;
        let resumed = restarted.register_or_resume_exact(metadata.clone(), at)?;
        assert_eq!(resumed.revision(), metadata.revision());
        let next = restarted.begin_next_session(
            &resumed,
            SessionId::new(SourceIdentifier::try_from("session-8")?),
            at,
        )?;
        assert_eq!(next.generation(), ConnectionGeneration::new(8)?);
        restarted.end_session(&next, at)?;
        drop(next);
        drop(resumed);
        restarted.shutdown()?;

        let mut changed = durable_registry_with_test_store(store)?;
        assert!(matches!(
            changed.register_or_resume_exact(
                direct_metadata_with_revision_evidence("restart-resume", "revision-1", 99)?,
                at,
            ),
            Err(RegistryError::RevisionEvidenceMismatch)
        ));
        Ok(())
    }

    #[test]
    fn legacy_revision_history_loads_but_cannot_exact_resume_without_evidence() -> TestResult {
        let at = Timestamp::from_unix_nanos(1_000_000_000);
        let metadata = direct_metadata("legacy-restart", "revision-1")?;
        let legacy_state = RegistryAuthorityState::try_new(
            vec![PersistedSourceAuthority {
                source_id: metadata.source_id().clone(),
                used_revisions: BoundedVec::try_new(vec![metadata.revision().clone()])?,
                latest_revision_evidence: None,
                revoked: false,
                last_epoch: 1,
                generation_high_water: None,
            }],
            Vec::new(),
        )?;
        let legacy_wire = serde_json::to_vec(&legacy_state)?;
        let loaded: RegistryAuthorityState = serde_json::from_slice(&legacy_wire)?;
        let mut restarted =
            AuthoritativeSourceRegistry::try_new_ephemeral_with_authority_state_for_diagnostics(
                loaded,
            )?;

        assert!(matches!(
            restarted.register_or_resume_exact(metadata, at),
            Err(RegistryError::RevisionEvidenceUnavailable)
        ));
        Ok(())
    }

    #[test]
    fn restart_rejects_stale_revision_and_exhausted_generation_without_mutation() -> TestResult {
        let at = Timestamp::from_unix_nanos(1_000_000_000);
        let store = Arc::new(FailingAuthorityStore::default());
        let mut first = durable_registry_with_test_store(store.clone())?;
        let revision_1 = direct_metadata("restart-stale", "revision-1")?;
        let registered = first.register_or_resume_exact(revision_1.clone(), at)?;
        let revision_2 = direct_metadata("restart-stale", "revision-2")?;
        let replacement = first.replace_metadata(&registered, revision_2.clone(), at)?;
        let maximum = first.begin_session(
            &replacement,
            SessionId::new(SourceIdentifier::try_from("maximum-generation")?),
            ConnectionGeneration::new(u64::MAX)?,
            at,
        )?;
        first.end_session(&maximum, at)?;
        drop(maximum);
        drop(replacement);
        drop(registered);
        first.shutdown()?;

        let mut restarted = durable_registry_with_test_store(store.clone())?;
        assert!(matches!(
            restarted.register_or_resume_exact(revision_1, at),
            Err(RegistryError::RevisionNotLatest)
        ));
        let resumed = restarted.register_or_resume_exact(revision_2, at)?;
        let before = restarted.export_authority_state()?;
        let stores_before = store.store_calls.load(Ordering::Acquire);
        assert!(matches!(
            restarted.begin_next_session(
                &resumed,
                SessionId::new(SourceIdentifier::try_from("never-started")?),
                at,
            ),
            Err(RegistryError::ConnectionGenerationExhausted)
        ));
        assert_eq!(restarted.export_authority_state()?, before);
        assert_eq!(store.store_calls.load(Ordering::Acquire), stores_before);
        let entry = restarted
            .entries
            .get(resumed.source_id())
            .ok_or("resumed entry disappeared")?;
        assert!(entry.active.is_none());
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

    #[test]
    fn exclusive_installed_replacement_restores_nonempty_state_rejected_by_default() -> TestResult {
        let at = Timestamp::from_unix_nanos(1_000_000_000);
        let crashed_store = Arc::new(FailingAuthorityStore::default());
        let metadata = direct_metadata_with_provider_and_limit(
            "installed-crash-recovery",
            "revision-1",
            "installed-crash-recovery-provider",
            2,
        )?;
        let mut crashed = durable_registry_with_test_store(crashed_store.clone())?;
        let registered = crashed.register_or_resume_exact(metadata.clone(), at)?;
        let expected = crashed.export_authority_state()?;
        assert!(!expected.sources.is_empty());
        assert!(!expected.budget_policies.is_empty());
        let crashed_permit = match registered
            .budget()
            .ok_or("crashed provider budget was absent")?
            .try_acquire()
        {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("crashed provider budget was unusable: {other:?}").into()),
        };
        let payload = crashed_store
            .payload
            .lock()
            .map_err(|_| "crashed authority payload lock was poisoned")?
            .clone()
            .ok_or("crashed authority payload was absent")?;
        crashed_permit.release();
        drop(registered);
        crashed.shutdown()?;
        let replacement_store = Arc::new(FailingAuthorityStore {
            payload: Mutex::new(Some(payload)),
            ..FailingAuthorityStore::default()
        });
        assert!(matches!(
            durable_registry_with_test_store(replacement_store.clone()),
            Err(RegistryError::UncleanAuthorityPredecessor)
        ));
        let rejected_payload = replacement_store
            .payload
            .lock()
            .map_err(|_| "rejected authority payload lock was poisoned")?
            .clone()
            .ok_or("rejected authority payload was absent")?;
        let predecessor_envelope: serde_json::Value = serde_json::from_slice(&rejected_payload)?;
        let mut replacement = durable_registry_with_test_store_for_exclusive_installed_replacement(
            replacement_store.clone(),
        )?;
        let recovered_payload = replacement_store
            .payload
            .lock()
            .map_err(|_| "replacement authority payload lock was poisoned")?
            .clone()
            .ok_or("replacement authority payload was absent")?;
        let recovered_envelope: serde_json::Value = serde_json::from_slice(&recovered_payload)?;
        let predecessor_generation = predecessor_envelope["run_generation"]
            .as_u64()
            .ok_or("predecessor run generation was invalid")?;
        assert_eq!(
            recovered_envelope["run_generation"],
            predecessor_generation
                .checked_add(1)
                .ok_or("test run generation overflowed")?
        );
        assert_eq!(recovered_envelope["run_state"], "in_use");
        assert_eq!(
            recovered_envelope["saved_at_wall"],
            recovered_envelope["wall_high_water"]
        );
        let mut expected_recovered_envelope = predecessor_envelope;
        expected_recovered_envelope["run_generation"] =
            recovered_envelope["run_generation"].clone();
        expected_recovered_envelope["saved_at_wall"] = recovered_envelope["saved_at_wall"].clone();
        expected_recovered_envelope["wall_high_water"] =
            recovered_envelope["wall_high_water"].clone();
        for group in expected_recovered_envelope["budgets"]
            .as_array_mut()
            .ok_or("predecessor budgets were invalid")?
        {
            let checkpoint = group
                .get_mut("checkpoint")
                .ok_or("predecessor budget checkpoint was absent")?;
            let in_flight = checkpoint["in_flight"]
                .as_u64()
                .ok_or("predecessor in-flight count was invalid")?;
            if in_flight != 0 {
                checkpoint["in_flight"] = serde_json::Value::from(0);
            }
            let terminalized = checkpoint["terminal"] == serde_json::Value::Bool(true)
                && checkpoint["poisoned"] == serde_json::Value::Bool(true)
                && checkpoint["disabled"] == serde_json::Value::Bool(true);
            if terminalized {
                checkpoint["terminal"] = serde_json::Value::Bool(false);
                checkpoint["poisoned"] = serde_json::Value::Bool(false);
                checkpoint["disabled"] = serde_json::Value::Bool(false);
            }
            if in_flight != 0 || terminalized {
                let generation = checkpoint["availability_generation"]
                    .as_u64()
                    .ok_or("predecessor availability generation was invalid")?;
                checkpoint["availability_generation"] = serde_json::Value::from(
                    generation
                        .checked_add(1)
                        .ok_or("test availability generation overflowed")?,
                );
            }
        }
        assert_eq!(recovered_envelope, expected_recovered_envelope);
        assert_eq!(replacement.export_authority_state()?, expected);
        let resumed = replacement.register_or_resume_exact(metadata, at)?;
        let permit = match resumed
            .budget()
            .ok_or("restored provider budget was absent")?
            .try_acquire()
        {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("restored provider budget was unusable: {other:?}").into()),
        };
        permit.release();
        assert!(matches!(
            resumed
                .budget()
                .ok_or("restored provider budget disappeared")?
                .try_acquire(),
            BudgetDecision::WaitUntil(_)
        ));
        drop(resumed);
        replacement.shutdown()?;
        Ok(())
    }

    include!("tests/temporal_cases.rs");
    include!("tests/extraction_authority.rs");
    mod time_cases {
        use super::*;

        include!("tests/time_cases.rs");
    }

    fn rule(value: &str) -> Result<IntegrityRule, Box<dyn std::error::Error>> {
        Ok(IntegrityRule::new(
            SourceIdentifier::try_from(value)?,
            RuleVersion::new(1)?,
        ))
    }
}
