mod support;

use std::error::Error;

use market_squawk_domain::{
    AssessmentStatus, CoverageConsolidation, CoverageDelay, CoverageDimension, CoverageError,
    CoverageScope, CoverageStatus, LiveEventClass, LiveEvidenceBinding, MarketDepth,
    MetadataRevision, ProviderChannel, ProviderProduct, QualificationAssessment,
    QualificationComponent, QualificationError, SnapshotEvidence, SourceCoverageRecord, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use support::live::{
    BindingSpec, ChecksumFixture, Component, RelationalEvidenceSpec, SnapshotPolicyFixture,
    assessment_input, assessment_input_with_relations, binding, valid_assessment_input,
};

#[test]
fn archive_assessment_never_returns_execution_authority() -> Result<(), Box<dyn Error>> {
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;

    assert_eq!(
        assessment.recorded_quality(),
        market_squawk_domain::DataQuality::DirectVerified
    );
    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_020)),
        AssessmentStatus::Satisfied
    );
    Ok(())
}

#[test]
fn coverage_rejects_source_and_channel_transplants() -> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let scope = CoverageScope::new(
        base.source_id().clone(),
        base.venue_id().clone(),
        base.provider_product().clone(),
        base.provider_channel().clone(),
        base.event_class(),
        base.book_state()
            .map(market_squawk_domain::BookStateBinding::depth),
        CoverageDelay::RealTime,
        CoverageConsolidation::SingleVenue,
        Timestamp::from_unix_nanos(900),
        Some(Timestamp::from_unix_nanos(2_000)),
        base.metadata_revision().clone(),
    )?;

    for replacement in [
        binding(&BindingSpec {
            source: "kraken-direct",
            ..BindingSpec::default()
        })?,
        binding(&BindingSpec {
            channel: "level3",
            ..BindingSpec::default()
        })?,
    ] {
        assert!(
            SourceCoverageRecord::new(replacement, scope.clone(), CoverageStatus::Sufficient)
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn coverage_rejects_transplants_across_every_binding_dimension() -> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let scope = |source: &str,
                 venue: &str,
                 product: &str,
                 channel: &str,
                 event_class: LiveEventClass,
                 depth: MarketDepth,
                 revision: &str|
     -> Result<CoverageScope, Box<dyn Error>> {
        Ok(CoverageScope::new(
            SourceId::try_from(source)?,
            VenueId::try_from(venue)?,
            ProviderProduct::new(SourceIdentifier::try_from(product)?),
            ProviderChannel::new(SourceIdentifier::try_from(channel)?),
            event_class,
            Some(depth),
            CoverageDelay::RealTime,
            CoverageConsolidation::SingleVenue,
            Timestamp::from_unix_nanos(900),
            Some(Timestamp::from_unix_nanos(2_000)),
            MetadataRevision::new(SourceIdentifier::try_from(revision)?),
        )?)
    };
    let cases = [
        (
            scope(
                "kraken-direct",
                "COINBASE",
                "BTC-USD",
                "level2",
                LiveEventClass::BookDelta,
                MarketDepth::PriceLevel,
                "coinbase-advanced-trade-v3",
            )?,
            CoverageDimension::Source,
        ),
        (
            scope(
                "coinbase-direct",
                "KRAKEN",
                "BTC-USD",
                "level2",
                LiveEventClass::BookDelta,
                MarketDepth::PriceLevel,
                "coinbase-advanced-trade-v3",
            )?,
            CoverageDimension::Venue,
        ),
        (
            scope(
                "coinbase-direct",
                "COINBASE",
                "ETH-USD",
                "level2",
                LiveEventClass::BookDelta,
                MarketDepth::PriceLevel,
                "coinbase-advanced-trade-v3",
            )?,
            CoverageDimension::Product,
        ),
        (
            scope(
                "coinbase-direct",
                "COINBASE",
                "BTC-USD",
                "level3",
                LiveEventClass::BookDelta,
                MarketDepth::PriceLevel,
                "coinbase-advanced-trade-v3",
            )?,
            CoverageDimension::Channel,
        ),
        (
            scope(
                "coinbase-direct",
                "COINBASE",
                "BTC-USD",
                "level2",
                LiveEventClass::BookSnapshot,
                MarketDepth::PriceLevel,
                "coinbase-advanced-trade-v3",
            )?,
            CoverageDimension::EventClass,
        ),
        (
            scope(
                "coinbase-direct",
                "COINBASE",
                "BTC-USD",
                "level2",
                LiveEventClass::BookDelta,
                MarketDepth::OrderLevel,
                "coinbase-advanced-trade-v3",
            )?,
            CoverageDimension::Depth,
        ),
        (
            scope(
                "coinbase-direct",
                "COINBASE",
                "BTC-USD",
                "level2",
                LiveEventClass::BookDelta,
                MarketDepth::PriceLevel,
                "coinbase-advanced-trade-v4",
            )?,
            CoverageDimension::MetadataRevision,
        ),
    ];

    for (transplanted, dimension) in cases {
        assert_eq!(
            SourceCoverageRecord::new(base.clone(), transplanted, CoverageStatus::Sufficient,),
            Err(CoverageError::BindingMismatch(dimension))
        );
    }
    Ok(())
}

#[test]
fn non_book_binding_rejects_retained_book_state() -> Result<(), Box<dyn Error>> {
    let book = binding(&BindingSpec::default())?;
    let mut wire = serde_json::to_value(book)?;
    wire["event_class"] = serde_json::json!("trade");

    assert!(serde_json::from_value::<LiveEvidenceBinding>(wire).is_err());
    Ok(())
}

#[test]
fn non_book_coverage_rejects_market_depth() -> Result<(), Box<dyn Error>> {
    assert!(
        CoverageScope::new(
            SourceId::try_from("coinbase-direct")?,
            VenueId::try_from("COINBASE")?,
            ProviderProduct::new(SourceIdentifier::try_from("BTC-USD")?),
            market_squawk_domain::ProviderChannel::new(SourceIdentifier::try_from("ticker")?),
            LiveEventClass::Trade,
            Some(MarketDepth::TopOfBook),
            CoverageDelay::RealTime,
            CoverageConsolidation::SingleVenue,
            Timestamp::from_unix_nanos(900),
            None,
            market_squawk_domain::MetadataRevision::new(SourceIdentifier::try_from(
                "coinbase-advanced-trade-v3",
            )?),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn every_assessment_component_rejects_a_transplanted_binding() -> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let replacement = binding(&BindingSpec {
        channel: "ticker",
        ..BindingSpec::default()
    })?;
    let cases = [
        (
            Component::SourcePolicy,
            QualificationComponent::SourcePolicy,
        ),
        (Component::Sequence, QualificationComponent::Sequence),
        (Component::Snapshot, QualificationComponent::Snapshot),
        (Component::Checksum, QualificationComponent::Checksum),
        (Component::Timing, QualificationComponent::Timing),
        (
            Component::TradingStatus,
            QualificationComponent::TradingStatus,
        ),
        (Component::Precision, QualificationComponent::Precision),
        (Component::Coverage, QualificationComponent::Coverage),
        (Component::Book, QualificationComponent::Book),
        (Component::Stream, QualificationComponent::Stream),
        (Component::Capture, QualificationComponent::Capture),
    ];

    for (component, expected) in cases {
        let input = assessment_input(
            base.clone(),
            Some(component),
            replacement.clone(),
            Timestamp::from_unix_nanos(1_020),
        )?;
        assert_eq!(
            QualificationAssessment::try_from(input),
            Err(QualificationError::BindingMismatch {
                component: expected
            })
        );
    }
    Ok(())
}

#[test]
fn complete_key_rejects_transplant_across_every_identity_dimension() -> Result<(), Box<dyn Error>> {
    let base_spec = BindingSpec::default();
    let base = binding(&base_spec)?;
    let mutations = [
        BindingSpec {
            source: "kraken-direct",
            ..base_spec.clone()
        },
        BindingSpec {
            session: "session-8",
            ..base_spec.clone()
        },
        BindingSpec {
            metadata_revision: "coinbase-advanced-trade-v4",
            ..base_spec.clone()
        },
        BindingSpec {
            authorization_basis: "different-authorized-account",
            ..base_spec.clone()
        },
        BindingSpec {
            venue: "KRAKEN",
            ..base_spec.clone()
        },
        BindingSpec {
            instrument: "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cc",
            ..base_spec.clone()
        },
        BindingSpec {
            generation: 8,
            ..base_spec.clone()
        },
        BindingSpec {
            product: "ETH-USD",
            ..base_spec.clone()
        },
        BindingSpec {
            channel: "ticker",
            ..base_spec.clone()
        },
        BindingSpec {
            event_class: LiveEventClass::BookSnapshot,
            ..base_spec.clone()
        },
        BindingSpec {
            source_identifier: "update-43",
            ..base_spec.clone()
        },
        BindingSpec {
            payload_digest: 9,
            ..base_spec.clone()
        },
        BindingSpec {
            state_digest: 9,
            ..base_spec.clone()
        },
        BindingSpec {
            book_state_id: "book-state-43",
            ..base_spec.clone()
        },
        BindingSpec {
            depth: market_squawk_domain::MarketDepth::OrderLevel,
            ..base_spec
        },
    ];

    for mutation in mutations {
        let replacement = binding(&mutation)?;
        let input = assessment_input(
            base.clone(),
            Some(Component::Sequence),
            replacement,
            Timestamp::from_unix_nanos(1_020),
        )?;
        assert_eq!(
            QualificationAssessment::try_from(input),
            Err(QualificationError::BindingMismatch {
                component: QualificationComponent::Sequence,
            })
        );
    }
    Ok(())
}

#[test]
fn generation_rollover_invalidates_prior_assessment_without_aliasing() -> Result<(), Box<dyn Error>>
{
    let base = binding(&BindingSpec::default())?;
    let next_generation = binding(&BindingSpec {
        generation: 8,
        ..BindingSpec::default()
    })?;
    let input = assessment_input(
        base,
        Some(Component::Timing),
        next_generation,
        Timestamp::from_unix_nanos(1_020),
    )?;

    assert_eq!(
        QualificationAssessment::try_from(input),
        Err(QualificationError::BindingMismatch {
            component: QualificationComponent::Timing
        })
    );
    Ok(())
}

#[test]
fn snapshot_initialization_is_explicit_even_without_provider_sequence() -> Result<(), Box<dyn Error>>
{
    let generation = market_squawk_domain::ConnectionGeneration::new(7)?;
    let initialized = market_squawk_domain::InitializedSnapshot::new(
        generation,
        market_squawk_domain::SourceIdentifier::try_from("snapshot-7")?,
        market_squawk_domain::CanonicalStateDigest::new(
            market_squawk_domain::EvidenceDigest::new(
                market_squawk_domain::PayloadHashAlgorithm::Sha256,
                [7; 32],
            ),
            market_squawk_domain::CanonicalizationRule::new(
                market_squawk_domain::SourceIdentifier::try_from(
                    "market-squawk.book.price-level-v1",
                )?,
                market_squawk_domain::RuleVersion::new(1)?,
            ),
        ),
        Timestamp::from_unix_nanos(900),
        None,
    );
    let evidence = SnapshotEvidence::assess_initialized(initialized, generation, None)?;

    assert!(evidence.is_initialized());
    assert_eq!(evidence.snapshot_sequence(), None);
    assert!(!SnapshotEvidence::uninitialized(generation).is_initialized());
    Ok(())
}

#[test]
fn non_book_events_require_explicit_metadata_backed_snapshot_non_applicability()
-> Result<(), Box<dyn Error>> {
    let spec = BindingSpec {
        event_class: LiveEventClass::Trade,
        ..BindingSpec::default()
    };
    let base = binding(&spec)?;
    let input = assessment_input(base.clone(), None, base, Timestamp::from_unix_nanos(1_020))?;
    let assessment = QualificationAssessment::try_from(input)?;

    assert_eq!(
        assessment.assessment_status_at(Timestamp::from_unix_nanos(1_020)),
        AssessmentStatus::Satisfied
    );
    Ok(())
}

#[test]
fn checksum_target_and_book_integrity_follow_event_semantics() -> Result<(), Box<dyn Error>> {
    let book_binding = binding(&BindingSpec::default())?;
    let mut book_with_payload = RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    book_with_payload.checksum = ChecksumFixture::Payload;
    assert!(
        QualificationAssessment::try_from(assessment_input_with_relations(
            book_binding.clone(),
            None,
            book_binding.clone(),
            Timestamp::from_unix_nanos(1_020),
            book_with_payload,
        )?)
        .is_err()
    );

    let mut book_not_applicable = RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    book_not_applicable.book_integrity = market_squawk_domain::BookIntegrity::NotApplicable;
    assert!(
        QualificationAssessment::try_from(assessment_input_with_relations(
            book_binding.clone(),
            None,
            book_binding,
            Timestamp::from_unix_nanos(1_020),
            book_not_applicable,
        )?)
        .is_err()
    );

    let trade_binding = binding(&BindingSpec {
        event_class: LiveEventClass::Trade,
        ..BindingSpec::default()
    })?;
    let mut trade_with_book = RelationalEvidenceSpec::for_event(LiveEventClass::Trade);
    trade_with_book.checksum = ChecksumFixture::Book;
    assert!(
        QualificationAssessment::try_from(assessment_input_with_relations(
            trade_binding.clone(),
            None,
            trade_binding.clone(),
            Timestamp::from_unix_nanos(1_020),
            trade_with_book,
        )?)
        .is_err()
    );

    let mut trade_book_consistent = RelationalEvidenceSpec::for_event(LiveEventClass::Trade);
    trade_book_consistent.book_integrity = market_squawk_domain::BookIntegrity::Consistent;
    assert!(
        QualificationAssessment::try_from(assessment_input_with_relations(
            trade_binding.clone(),
            None,
            trade_binding,
            Timestamp::from_unix_nanos(1_020),
            trade_book_consistent,
        )?)
        .is_err()
    );
    Ok(())
}

#[test]
fn snapshot_policy_is_required_for_books_and_not_applicable_for_non_books()
-> Result<(), Box<dyn Error>> {
    let book_binding = binding(&BindingSpec::default())?;
    let mut book_not_applicable = RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    book_not_applicable.snapshot_policy = SnapshotPolicyFixture::NotApplicable;
    book_not_applicable.snapshot_initialized = false;
    book_not_applicable.snapshot_sequence = None;
    book_not_applicable.snapshot_observed = None;
    assert!(
        QualificationAssessment::try_from(assessment_input_with_relations(
            book_binding.clone(),
            None,
            book_binding,
            Timestamp::from_unix_nanos(1_020),
            book_not_applicable,
        )?)
        .is_err()
    );

    let trade_binding = binding(&BindingSpec {
        event_class: LiveEventClass::Trade,
        ..BindingSpec::default()
    })?;
    let mut trade_required = RelationalEvidenceSpec::for_event(LiveEventClass::Trade);
    trade_required.snapshot_policy = SnapshotPolicyFixture::Required;
    trade_required.snapshot_initialized = true;
    trade_required.snapshot_sequence = Some(40);
    trade_required.snapshot_observed = Some(42);
    assert!(
        QualificationAssessment::try_from(assessment_input_with_relations(
            trade_binding.clone(),
            None,
            trade_binding,
            Timestamp::from_unix_nanos(1_020),
            trade_required,
        )?)
        .is_err()
    );
    Ok(())
}

#[test]
fn metadata_backed_unsupported_checksum_remains_explicitly_supported() -> Result<(), Box<dyn Error>>
{
    for event_class in [LiveEventClass::BookDelta, LiveEventClass::Trade] {
        let evidence_binding = binding(&BindingSpec {
            event_class,
            ..BindingSpec::default()
        })?;
        let mut spec = RelationalEvidenceSpec::for_event(event_class);
        spec.checksum = ChecksumFixture::Unsupported;
        let assessment = QualificationAssessment::try_from(assessment_input_with_relations(
            evidence_binding.clone(),
            None,
            evidence_binding,
            Timestamp::from_unix_nanos(1_020),
            spec,
        )?)?;
        assert_eq!(
            assessment.recorded_quality(),
            market_squawk_domain::DataQuality::DirectVerified
        );
    }
    Ok(())
}

#[test]
fn metadata_backed_unsupported_sequence_is_retained_but_never_direct_verified()
-> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let mut spec = RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    spec.sequence_unsupported = true;
    spec.snapshot_sequence = None;
    spec.snapshot_observed = None;
    let assessment = QualificationAssessment::try_from(assessment_input_with_relations(
        base.clone(),
        None,
        base,
        Timestamp::from_unix_nanos(1_020),
        spec,
    )?)?;

    assert_eq!(
        assessment.recorded_quality(),
        market_squawk_domain::DataQuality::DirectUnverified
    );
    assert!(assessment.has_failure(market_squawk_domain::EligibilityFailure::SequenceIntegrity));
    Ok(())
}

#[test]
fn provided_book_sequences_reject_every_partial_or_contradictory_option_pair()
-> Result<(), Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    let mut cases = Vec::new();

    let mut sequence_snapshot_missing =
        RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    sequence_snapshot_missing.sequence_snapshot = None;
    cases.push(sequence_snapshot_missing);

    let mut snapshot_snapshot_missing =
        RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    snapshot_snapshot_missing.snapshot_sequence = None;
    cases.push(snapshot_snapshot_missing);

    let mut snapshot_sequences_differ =
        RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    snapshot_sequences_differ.snapshot_sequence = Some(39);
    cases.push(snapshot_sequences_differ);

    let mut snapshot_observed_missing =
        RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    snapshot_observed_missing.snapshot_observed = None;
    cases.push(snapshot_observed_missing);

    let mut sequence_observed_missing =
        RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    sequence_observed_missing.sequence_uninitialized = true;
    sequence_observed_missing.sequence_previous = None;
    sequence_observed_missing.sequence_observed = None;
    cases.push(sequence_observed_missing);

    let mut observed_sequences_differ =
        RelationalEvidenceSpec::for_event(LiveEventClass::BookDelta);
    observed_sequences_differ.snapshot_observed = Some(43);
    cases.push(observed_sequences_differ);

    for spec in cases {
        assert!(
            QualificationAssessment::try_from(assessment_input_with_relations(
                base.clone(),
                None,
                base.clone(),
                Timestamp::from_unix_nanos(1_020),
                spec,
            )?)
            .is_err()
        );
    }
    Ok(())
}
