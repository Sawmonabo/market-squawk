#[test]
fn current_authority_is_scoped_by_venue_instrument_event_and_depth() -> TestResult {
    use std::str::FromStr;

    use market_squawk_domain::{
        AggressorSide, InstrumentId, IntegrityRule, LiveEventClass, RuleVersion, SequenceNumber,
        VenueId,
    };
    use market_squawk_sources::{
        DecodeOutcome, DecodedProviderBatch, DecoderEvidence, ProviderAggressorEvidence,
        ProviderChecksumEvidence, ProviderDecimalLexeme, ProviderNormalizedObservation,
        ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
        ProviderSnapshotEvidence, ProviderTimestampEvidence, ValidatedSessionDecodeOutcome,
    };

    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut covered_instruments = (1_u128..4_096)
        .map(|value| InstrumentId::from_str(&format!("{value:032x}")))
        .collect::<Result<Vec<_>, _>>()?;
    let other_instrument = *covered_instruments
        .first()
        .ok_or("maximum-universe fixture must not be empty")?;
    covered_instruments.push(instrument);
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        direct_metadata_with_instruments("source-a", "revision-a", 0, None, covered_instruments)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut capture_control, mut capture_admission, _degrade) = capabilities.into_parts();
    capture_control.mark_healthy()?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut health_reporter = registry.take_current_health_reporter(&session)?;
    let health_at = now_timestamp()?;
    let valid_until = health_at.checked_add_nanos(10_000_000_000)?;
    let first_frame_at = health_at.checked_add_nanos(1)?;
    let current_frame_at = health_at.checked_add_nanos(2)?;
    assert!(matches!(
        registry.validate_current_authority(&session),
        Err(RegistryError::HealthNotQualified)
    ));
    let health = SourceHealthSnapshot::try_new(
        &session,
        health_at,
        ConnectionLiveness::Live {
            last_activity_at: health_at,
        },
        Some(health_at),
        Some(health_at),
        Some(health_at),
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
        AuthorizationHealth::Valid {
            evidence: exact_evidence(11),
            valid_until,
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until,
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?;
    let update = health_reporter.report(health)?;
    registry.record_health(&session, update)?;
    let current = registry.validate_current_authority(&session)?;
    current.validate_live_scope(
        &VenueId::try_from("coinbase")?,
        instrument,
        LiveEventClass::Trade,
        None,
    )?;
    let first_frame = frames.try_frame(
        TransportFrameKind::Binary,
        Bytes::from_static(b"same-payload"),
    )?;
    let second_frame = frames.try_frame(
        TransportFrameKind::Binary,
        Bytes::from_static(b"same-payload"),
    )?;
    capture_admission.preflight(&first_frame)?;
    let (resident, error_path_resident_drops) = observed_resident_generation_lease();
    let receipt = capture_admission.issue_after_enqueue(&first_frame, resident)?;
    capture_admission.validate_active(&first_frame)?;
    let validated = session.validate_live_frame(&second_frame)?;
    let decoder_rule =
        IntegrityRule::new(source_identifier("coinbase-decoder")?, RuleVersion::new(1)?);
    let evidence = DecoderEvidence::from_validated_frame(&validated, decoder_rule);
    let rule = |name: &str| -> TestResult<IntegrityRule> {
        Ok(IntegrityRule::new(
            source_identifier(name)?,
            RuleVersion::new(1)?,
        ))
    };
    let observation = ProviderNormalizedObservation::try_new(
        source_identifier("trade-1")?,
        VenueId::try_from("coinbase")?,
        instrument,
        ProviderTimestampEvidence::Provided {
            value: first_frame_at,
            rule: rule("coinbase-timestamp")?,
        },
        ProviderSequenceEvidence::Provided {
            value: SequenceNumber::new(1),
            rule: rule("coinbase-sequence")?,
        },
        ProviderSnapshotEvidence::NotApplicable(rule("trade-no-snapshot-v1")?),
        ProviderChecksumEvidence::Unsupported {
            rule: rule("coinbase-no-checksum")?,
        },
        ProviderObservationPayload::Trade {
            trade_id: source_identifier("trade-1")?,
            price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
            quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
            aggressor: ProviderAggressorEvidence::new(
                AggressorSide::Buy,
                Some(source_identifier("BUY")?),
                rule("coinbase-aggressor")?,
            ),
            taker_order_type: None,
        },
    )?;
    let batch = DecodedProviderBatch::try_new(evidence, vec![observation])?;
    let validated_session = registry.validate_session(&session, second_frame.received_at())?;
    assert!(matches!(
        validated_session.validate_decode_outcome_owned(DecodeOutcome::Data(batch), receipt),
        Err(RegistryError::CaptureReceiptMismatch)
    ));
    assert_eq!(error_path_resident_drops.load(Ordering::SeqCst), 1);
    assert!(
        current
            .validate_live_scope(
                &VenueId::try_from("kraken")?,
                instrument,
                LiveEventClass::Trade,
                None,
            )
            .is_err()
    );

    let current_frame = frames.try_frame(
        TransportFrameKind::Binary,
        Bytes::from_static(b"current-payload"),
    )?;
    capture_admission.preflight(&current_frame)?;
    let (resident, success_path_resident_drops) = observed_resident_generation_lease();
    let current_receipt = capture_admission.issue_after_enqueue(&current_frame, resident)?;
    capture_admission.validate_active(&current_frame)?;
    let current_validated = session.validate_live_frame(&current_frame)?;
    let current_evidence = DecoderEvidence::from_validated_frame(
        &current_validated,
        IntegrityRule::new(source_identifier("coinbase-decoder")?, RuleVersion::new(1)?),
    );
    let current_payload_digest = current_evidence.payload_digest();
    let make_current_observation =
        |instrument: InstrumentId, trade_id: &str, sequence: u64| -> TestResult<_> {
            Ok(ProviderNormalizedObservation::try_new(
                source_identifier(trade_id)?,
                VenueId::try_from("coinbase")?,
                instrument,
                ProviderTimestampEvidence::Provided {
                    value: current_frame_at,
                    rule: rule("coinbase-timestamp")?,
                },
                ProviderSequenceEvidence::Provided {
                    value: SequenceNumber::new(sequence),
                    rule: rule("coinbase-sequence")?,
                },
                ProviderSnapshotEvidence::NotApplicable(rule("trade-no-snapshot-v1")?),
                ProviderChecksumEvidence::Unsupported {
                    rule: rule("coinbase-no-checksum")?,
                },
                ProviderObservationPayload::Trade {
                    trade_id: source_identifier(trade_id)?,
                    price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
                    quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
                    aggressor: ProviderAggressorEvidence::new(
                        AggressorSide::Buy,
                        Some(source_identifier("BUY")?),
                        rule("coinbase-aggressor")?,
                    ),
                    taker_order_type: None,
                },
            )?)
        };
    let first_current_observation = make_current_observation(instrument, "trade-2", 2)?;
    let other_observation = make_current_observation(other_instrument, "trade-3", 3)?;
    let second_current_observation = make_current_observation(instrument, "trade-4", 4)?;
    let validated_session = registry.validate_session(&session, current_frame.received_at())?;
    let validated_outcome = validated_session.validate_decode_outcome_owned(
        DecodeOutcome::Data(DecodedProviderBatch::try_new(
            current_evidence,
            vec![
                first_current_observation,
                other_observation,
                second_current_observation,
            ],
        )?),
        current_receipt,
    )?;
    let ValidatedSessionDecodeOutcome::Data(captured) = validated_outcome else {
        return Err("data outcome changed disposition".into());
    };
    let current_batches = current.validate_data_outcome_owned(captured)?;
    assert_eq!(success_path_resident_drops.load(Ordering::SeqCst), 1);
    let mut routed_batches = current_batches.into_iter();
    assert_eq!(routed_batches.len(), 2);
    let current_batch = routed_batches
        .next()
        .ok_or("current routing collection lost its first batch")?;
    assert_eq!(current_batch.key().instrument(), instrument);
    assert!(current_batch.retained_bytes() < 128 * 1024);
    let mut current_observations = current_batch.into_observations();
    assert_eq!(current_observations.len(), 2);
    let current_observation = current_observations
        .next()
        .ok_or("current batch lost its observation")?;
    assert_eq!(
        current_observation
            .observation()
            .source_identifier()
            .as_str(),
        "trade-2"
    );
    let coverage = current_observation.policy().coverage();
    assert_eq!(coverage.source_id().as_str(), "source-a");
    assert_eq!(coverage.venue().as_str(), "coinbase");
    assert_eq!(
        coverage.provider_product().as_source_identifier().as_str(),
        "direct-product"
    );
    assert_eq!(
        coverage.provider_channel().as_source_identifier().as_str(),
        "trades"
    );
    assert_eq!(coverage.event_class(), LiveEventClass::Trade);
    assert_eq!(coverage.depth(), None);
    assert_eq!(coverage.delay(), CoverageDelay::RealTime);
    assert_eq!(coverage.consolidation(), CoverageConsolidation::SingleVenue);
    assert_eq!(coverage.delivery(), DeliveryEvidence::DirectVenue);
    assert_eq!(coverage.evidence(), &exact_evidence(3));
    assert_eq!(coverage.effective_from(), Timestamp::from_unix_nanos(0));
    assert_eq!(coverage.effective_until(), None);
    assert_eq!(
        coverage.metadata_revision().as_source_identifier().as_str(),
        "revision-a"
    );
    assert_eq!(
        current_observation.frame_evidence().frame_id(),
        current_frame.frame_id()
    );
    assert_eq!(
        current_observation.frame_evidence().received_at(),
        current_frame.received_at()
    );
    assert_eq!(
        current_observation.frame_evidence().payload_digest(),
        current_payload_digest
    );
    assert!(
        current_observation
            .frame_evidence()
            .binding()
            .shares_allocation_with(current_frame.binding())
    );
    assert_eq!(
        current_observation
            .frame_evidence()
            .decoder_rule()
            .provider_rule()
            .as_str(),
        "coinbase-decoder"
    );
    let second_current_observation = current_observations
        .next()
        .ok_or("current batch lost its second observation")?;
    assert_eq!(
        second_current_observation
            .observation()
            .source_identifier()
            .as_str(),
        "trade-4"
    );
    let other_batch = routed_batches
        .next()
        .ok_or("current routing collection lost its second batch")?;
    assert_eq!(other_batch.key().instrument(), other_instrument);
    assert_eq!(other_batch.into_observations().len(), 1);
    Ok(())
}
