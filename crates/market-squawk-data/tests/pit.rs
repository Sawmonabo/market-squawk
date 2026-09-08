use std::error::Error;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::{Duration, Instant};

use market_squawk_data::{
    DatasetManifestRef, ObservationFamilyKey, PointInTimeCandidate, PointInTimeError,
    PointInTimeExclusionReason, PointInTimeLimits, PointInTimePolicy, PointInTimeRequest,
    PointInTimeRevisionMode, PointInTimeRevisionState, PointInTimeSelection, PointInTimeService,
    Sha256Digest,
};
use market_squawk_domain::{
    AlternativeDataObservation, AvailabilityEvidence, CalendarDate, CorporateActionKind,
    CorporateActionObservation, DataQuality, DigestAlgorithm, EffectiveInterval, FilingObservation,
    FundamentalAmendmentStatus, FundamentalCadence, FundamentalConsolidation,
    FundamentalDimensionContext, FundamentalFactContext, FundamentalFactContextInput,
    FundamentalObservation, FundamentalPeriod, FundamentalRestatementStatus,
    FundamentalRevisionOrder, InstrumentId, MacroObservation, PayloadHash, PayloadReference,
    PositionObservation, PositionSide, QuantityLots, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SchemaVersion, SourceId, SourceIdentifier, Timestamp, TransactionObservation,
    UniverseMembershipObservation, XbrlDimensionEvidence, XbrlDimensionLocation,
    XbrlDimensionMember, XbrlQualifiedName,
};
use market_squawk_sources::{CanonicalObservationFamily, CanonicalObservationPayload};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn transaction_family_identity_preserves_the_provenance_instrument() -> TestResult {
    let first_instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let second_instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c2")?;
    let transaction = |instrument_id| -> Result<PointInTimeCandidate, Box<dyn Error>> {
        let context = ResearchContext::new(
            ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id: SourceId::try_from("broker-export")?,
                instrument_id: Some(instrument_id),
                venue_id: None,
                source_identifier: SourceIdentifier::try_from("transaction-provenance")?,
                source_timestamp: None,
                received_at: timestamp(10),
                ingested_at: timestamp(11),
                quality: DataQuality::OfficialDelayed,
                payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                    DigestAlgorithm::Sha256,
                    [7; 32],
                )),
                availability: AvailabilityEvidence::local_first_observed(timestamp(10)),
            })?,
            ResearchTime::try_new_with_coordinates(exact(50), None, RevisionNumber::new(1)?, None)?,
        )?;
        Ok(PointInTimeCandidate::new(
            ResearchObservation::Transaction(TransactionObservation::new(
                context,
                SourceIdentifier::try_from("taxable-account")?,
                SourceIdentifier::try_from("trade")?,
                SourceIdentifier::try_from("broker-transaction-1")?,
            )),
            manifest(1, 1)?,
        ))
    };

    let first = transaction(first_instrument)?.family_key()?;
    let second = transaction(second_instrument)?.family_key()?;

    assert_ne!(first, second);
    Ok(())
}

#[tokio::test]
async fn source_revision_encodings_match_pit_for_every_observation_variant() -> TestResult {
    let instrument = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1")?;
    let source = SourceId::try_from("canonical-compatibility")?;
    let context = |source_record: &str,
                   instrument_id: Option<InstrumentId>|
     -> Result<ResearchContext, Box<dyn Error>> {
        Ok(ResearchContext::new(
            ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id: source.clone(),
                instrument_id,
                venue_id: None,
                source_identifier: SourceIdentifier::try_from(source_record)?,
                source_timestamp: None,
                received_at: timestamp(10),
                ingested_at: timestamp(11),
                quality: DataQuality::OfficialDelayed,
                payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                    DigestAlgorithm::Sha256,
                    [7; 32],
                )),
                availability: AvailabilityEvidence::local_first_observed(timestamp(10)),
            })?,
            ResearchTime::try_new_with_coordinates(exact(50), None, RevisionNumber::new(1)?, None)?,
        )?)
    };
    let fundamental_end = CalendarDate::new(2025, 12, 31)?;
    let fundamental = |source_record: &str,
                       accession: &str,
                       revision: u32,
                       value: Decimal,
                       dimensions: FundamentalDimensionContext,
                       consolidation: FundamentalConsolidation,
                       restatement_status: FundamentalRestatementStatus|
     -> Result<ResearchObservation, Box<dyn Error>> {
        let revision = RevisionNumber::new(revision)?;
        let fundamental_context = ResearchContext::new(
            context(source_record, Some(instrument))?
                .provenance()
                .clone(),
            ResearchTime::try_new_with_coordinates(
                ResearchTemporalCoordinate::calendar_date(fundamental_end),
                None,
                revision,
                None,
            )?,
        )?;
        let fact_context = FundamentalFactContext::try_new(FundamentalFactContextInput {
            schema_version: SchemaVersion::CURRENT,
            period: FundamentalPeriod::instant(fundamental_end),
            unit: SourceIdentifier::try_from("USD")?,
            accession: SourceIdentifier::try_from(accession)?,
            filing_form: None,
            amendment_status: FundamentalAmendmentStatus::Unavailable,
            filed_on: None,
            frame: None,
            fiscal_year: None,
            fiscal_period: None,
            cadence: FundamentalCadence::Unavailable,
            xbrl_context_id: Some(SourceIdentifier::try_from(format!("xbrl-{source_record}"))?),
            dimensions,
            consolidation,
            revision_order: FundamentalRevisionOrder::new(
                revision,
                SourceIdentifier::try_from("canonical-fixture-order-v1")?,
            ),
            restatement_status,
        })?;
        Ok(ResearchObservation::Fundamental(
            FundamentalObservation::new(
                fundamental_context,
                SourceIdentifier::try_from("us-gaap:Assets")?,
                value,
                fact_context,
            )?,
        ))
    };
    let empty_dimensions = FundamentalDimensionContext::try_source_reported(&[])?;
    let segment_dimension = XbrlDimensionEvidence::new(
        XbrlQualifiedName::try_new(
            "us-gaap:StatementBusinessSegmentsAxis",
            "https://fasb.org/us-gaap/2025",
        )?,
        XbrlDimensionMember::Explicit {
            member: XbrlQualifiedName::try_new(
                "example:ConsumerSegmentMember",
                "https://example.test/taxonomy/2025",
            )?,
        },
        XbrlDimensionLocation::Segment,
    );
    let base_fundamental = fundamental(
        "fundamental-base",
        "0000000000-25-000001",
        1,
        Decimal::new(12_345, 2),
        empty_dimensions.clone(),
        FundamentalConsolidation::Unavailable,
        FundamentalRestatementStatus::Unavailable,
    )?;
    let later_occurrence = fundamental(
        "fundamental-amendment",
        "0000000000-25-000002",
        2,
        Decimal::new(12_500, 2),
        empty_dimensions.clone(),
        FundamentalConsolidation::Unavailable,
        FundamentalRestatementStatus::SourceReported {
            restated: true,
            source_status: SourceIdentifier::try_from("source-restatement-v1")?,
        },
    )?;
    let dimensioned_fundamental = fundamental(
        "fundamental-segment",
        "0000000000-25-000001",
        1,
        Decimal::new(4_000, 2),
        FundamentalDimensionContext::try_source_reported(&[segment_dimension])?,
        FundamentalConsolidation::Unavailable,
        FundamentalRestatementStatus::Unavailable,
    )?;
    let nonconsolidated_fundamental = fundamental(
        "fundamental-nonconsolidated",
        "0000000000-25-000001",
        1,
        Decimal::new(8_345, 2),
        empty_dimensions,
        FundamentalConsolidation::SourceReportedNonConsolidated,
        FundamentalRestatementStatus::Unavailable,
    )?;
    let base_family = CanonicalObservationFamily::try_from_observation(&base_fundamental)?;
    assert_eq!(
        base_family,
        CanonicalObservationFamily::try_from_observation(&later_occurrence)?,
        "occurrence and revision evidence must not split a fundamental family"
    );
    assert_ne!(
        base_family,
        CanonicalObservationFamily::try_from_observation(&dimensioned_fundamental)?,
        "dimensional scope must remain in a fundamental family"
    );
    assert_ne!(
        base_family,
        CanonicalObservationFamily::try_from_observation(&nonconsolidated_fundamental)?,
        "consolidation scope must remain in a fundamental family"
    );
    let observations = vec![
        ResearchObservation::Filing(FilingObservation::new(
            context("filing-record", Some(instrument))?,
            SourceIdentifier::try_from("10-K")?,
            SourceIdentifier::try_from("0000000000-24-000001")?,
        )?),
        base_fundamental,
        later_occurrence,
        dimensioned_fundamental,
        nonconsolidated_fundamental,
        ResearchObservation::Macro(MacroObservation::new(
            context("macro-record", None)?,
            SourceIdentifier::try_from("GDP")?,
            Decimal::new(31_415, 3),
            SourceIdentifier::try_from("billions-usd")?,
        )),
        ResearchObservation::PortfolioPosition(PositionObservation::new(
            context("position-record", Some(instrument))?,
            SourceIdentifier::try_from("taxable-account")?,
            PositionSide::Long,
            QuantityLots::new(25)?,
        )?),
        ResearchObservation::Transaction(TransactionObservation::new(
            context("transaction-provenance", None)?,
            SourceIdentifier::try_from("taxable-account")?,
            SourceIdentifier::try_from("trade")?,
            SourceIdentifier::try_from("broker-transaction-1")?,
        )),
        ResearchObservation::CorporateAction(CorporateActionObservation::new(
            context("corporate-action-record", Some(instrument))?,
            CorporateActionKind::Delisting,
        )?),
        ResearchObservation::UniverseMembership(UniverseMembershipObservation::new(
            context("universe-membership-record", Some(instrument))?,
            SourceIdentifier::try_from("us-equities.historical")?,
            EffectiveInterval::new(timestamp(50), None)?,
        )?),
        ResearchObservation::AlternativeData(AlternativeDataObservation::new(
            context("alternative-record", None)?,
            SourceIdentifier::try_from("local-factors")?,
            SourceIdentifier::try_from("quality-score")?,
            Decimal::new(875, 3),
            None,
        )),
    ];
    let candidates = observations
        .into_iter()
        .enumerate()
        .map(|(index, observation)| {
            let marker = u8::try_from(index + 1)?;
            Ok(PointInTimeCandidate::new(
                observation,
                manifest(u64::from(marker), marker)?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let selection = select(
        &PointInTimeService::new(),
        &request(
            policy(PointInTimeRevisionMode::LatestKnown)?,
            timestamp(100),
            None,
            exact(100),
            None,
            limits(16, 16, 4, 16, 1 << 20)?,
        )?,
        &candidates,
    )
    .await?;

    assert_eq!(
        selection.records().len() + selection.exclusions().len(),
        candidates.len()
    );
    for record in selection.records().iter().copied().chain(
        selection
            .exclusions()
            .iter()
            .map(|excluded| excluded.record()),
    ) {
        let observation = record.candidate().observation();
        let family = CanonicalObservationFamily::try_from_observation(observation)?;
        let payload = CanonicalObservationPayload::try_from_observation(observation)?;
        assert!(family.exact_bytes().starts_with(b"MSQPIT"));
        assert!(payload.exact_bytes().starts_with(b"MSQPIT"));
        assert_eq!(family.identity().bytes(), record.family_identity().bytes());
        assert_eq!(
            payload.identity().bytes(),
            record.payload_identity().bytes()
        );
        if let ResearchObservation::AlternativeData(value) = observation {
            assert_eq!(
                payload,
                CanonicalObservationPayload::alternative_data(
                    value.dataset(),
                    value.field(),
                    value.value(),
                    value.unit(),
                )?
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn point_in_time_selection_is_causal_bounded_and_auditable() -> TestResult {
    let service = PointInTimeService::new();
    let manifest_a = manifest(7, 7)?;
    let manifest_b = manifest(8, 8)?;
    let selection_limits = limits(64, 64, 8, 64, 1 << 20)?;
    let latest = policy(PointInTimeRevisionMode::LatestKnown)?;
    let all_known = policy(PointInTimeRevisionMode::AllKnown)?;
    let unsupported_policy_version =
        NonZeroU32::new(2).ok_or("test policy version must be nonzero")?;
    assert!(matches!(
        PointInTimePolicy::try_new(
            unsupported_policy_version,
            PointInTimeRevisionMode::LatestKnown,
        ),
        Err(PointInTimeError::UnsupportedPolicyVersion { found: 2 })
    ));

    let as_of = timestamp(100);
    let effective_cutoff = exact(50);
    let base_request = request(
        latest,
        as_of,
        None,
        effective_cutoff.clone(),
        None,
        selection_limits,
    )?;
    let revision_one = macro_candidate(
        "GDP",
        "gdp-2024q1",
        10,
        1,
        exact(50),
        Some(exact(20)),
        Some(exact(80)),
        AvailabilityEvidence::evidenced(
            timestamp(20),
            SourceIdentifier::try_from("release-gdp-r1")?,
        ),
        manifest_a.clone(),
    )?;
    let revision_two = macro_candidate(
        "GDP",
        "gdp-2024q1",
        11,
        2,
        exact(50),
        Some(exact(60)),
        None,
        AvailabilityEvidence::local_first_observed(timestamp(60)),
        manifest_a.clone(),
    )?;
    let compacted_revision_two =
        PointInTimeCandidate::new(revision_two.observation().clone(), manifest_b.clone());
    assert_eq!(revision_one.family_key()?, revision_two.family_key()?);
    assert!(matches!(
        revision_two.family_key()?,
        ObservationFamilyKey::Macro { .. }
    ));

    let calendar_published = macro_candidate(
        "CPI",
        "cpi-2024m1",
        3,
        1,
        exact(40),
        Some(calendar(2024, 1, 15)?),
        None,
        AvailabilityEvidence::evidenced(timestamp(30), SourceIdentifier::try_from("release-cpi")?),
        manifest_a.clone(),
    )?;
    let base_candidates = vec![
        revision_one.clone(),
        revision_two.clone(),
        calendar_published.clone(),
    ];
    let original_candidates = base_candidates.clone();
    let selection = select(&service, &base_request, &base_candidates).await?;
    assert_eq!(base_candidates, original_candidates);
    assert_eq!(selection.records().len(), 2);
    assert_eq!(selection.exclusions().len(), 1);
    assert!(
        selection.exclusions()[0]
            .reasons()
            .contains(PointInTimeExclusionReason::SupersededByKnowledgeTime)
    );
    assert_eq!(
        selection.exclusion_counts().superseded_by_knowledge_time(),
        1
    );
    assert!(selection.records().iter().any(|record| {
        record.candidate() == &revision_two
            && record.revision_state() == PointInTimeRevisionState::Current
            && record.payload_identity() != record.provenance_identity()
    }));
    assert!(selection.records().iter().any(|record| {
        record.candidate() == &calendar_published
            && record.revision_state() == PointInTimeRevisionState::Current
    }));
    assert!(selection.retained_bytes() > 0);

    let mut reversed = base_candidates.clone();
    reversed.reverse();
    let reordered = select(&service, &base_request, &reversed).await?;
    assert_eq!(selection.content_identity(), reordered.content_identity());
    assert_eq!(selection.audit_identity(), reordered.audit_identity());

    let partition_a_candidates = [revision_two.clone(), calendar_published.clone()];
    let partition_a = select(&service, &base_request, &partition_a_candidates).await?;
    let partition_b_candidates = [compacted_revision_two.clone(), calendar_published.clone()];
    let partition_b = select(&service, &base_request, &partition_b_candidates).await?;
    assert_eq!(
        partition_a.content_identity(),
        partition_b.content_identity()
    );
    assert_ne!(partition_a.audit_identity(), partition_b.audit_identity());

    let future_revision = macro_candidate(
        "GDP",
        "gdp-2024q1",
        12,
        3,
        exact(50),
        Some(exact(120)),
        None,
        AvailabilityEvidence::evidenced(
            timestamp(120),
            SourceIdentifier::try_from("release-gdp-r3")?,
        ),
        manifest_a.clone(),
    )?;
    let mut future_perturbed = base_candidates.clone();
    future_perturbed.push(future_revision);
    let with_future = select(&service, &base_request, &future_perturbed).await?;
    assert_eq!(selection.content_identity(), with_future.content_identity());
    assert_ne!(selection.audit_identity(), with_future.audit_identity());
    let future_exclusion = with_future
        .exclusions()
        .iter()
        .find(|excluded| excluded.candidate().revision().get() == 3)
        .ok_or("future revision exclusion was not retained")?;
    assert!(
        future_exclusion
            .reasons()
            .contains(PointInTimeExclusionReason::AvailabilityAfterAsOf)
    );
    assert!(
        future_exclusion
            .reasons()
            .contains(PointInTimeExclusionReason::PublicationAfterAsOf)
    );

    let history_request = request(
        all_known,
        as_of,
        None,
        effective_cutoff.clone(),
        None,
        selection_limits,
    )?;
    let history_candidates = [revision_one.clone(), revision_two.clone()];
    let history = select(&service, &history_request, &history_candidates).await?;
    assert_eq!(history.records().len(), 2);
    assert_eq!(history.exclusions().len(), 0);
    assert_eq!(history.revision_counts().superseded_history(), 1);
    assert!(history.records().iter().any(|record| {
        record.candidate().revision().get() == 1
            && record.revision_state() == PointInTimeRevisionState::Superseded
    }));
    assert_ne!(selection.content_identity(), history.content_identity());

    let unknown = macro_candidate(
        "UNRATE",
        "unrate-2024m1",
        1,
        1,
        exact(40),
        None,
        None,
        AvailabilityEvidence::unknown(),
        manifest_a.clone(),
    )?;
    let inferred = macro_candidate(
        "PAYEMS",
        "payems-2024m1",
        2,
        1,
        exact(40),
        None,
        None,
        AvailabilityEvidence::inferred(timestamp(30), SourceIdentifier::try_from("calendar-v1")?),
        manifest_a.clone(),
    )?;
    let effective_late = macro_candidate(
        "PCE",
        "pce-2024m2",
        4,
        1,
        exact(51),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let effective_incomparable = macro_candidate(
        "CIVPART",
        "civpart-2024m1",
        5,
        1,
        calendar(2024, 1, 31)?,
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let supersession_incomparable = macro_candidate(
        "M2SL",
        "m2sl-2024m1",
        6,
        1,
        exact(40),
        None,
        Some(calendar(2024, 2, 1)?),
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let excluded_candidates = [
        unknown,
        inferred,
        effective_late,
        effective_incomparable,
        supersession_incomparable,
    ];
    let excluded = select(&service, &base_request, &excluded_candidates).await?;
    let counts = excluded.exclusion_counts();
    assert_eq!(counts.excluded_candidates(), 5);
    assert_eq!(counts.unknown_availability(), 1);
    assert_eq!(counts.inferred_availability(), 1);
    assert_eq!(counts.effective_after_cutoff(), 1);
    assert_eq!(counts.effective_incomparable(), 1);
    assert_eq!(counts.supersession_incomparable(), 1);

    let explicit_calendar_publication_request = request(
        latest,
        as_of,
        Some(calendar(2024, 1, 1)?),
        effective_cutoff.clone(),
        None,
        selection_limits,
    )?;
    let calendar_candidates = [calendar_published.clone()];
    let calendar_excluded = select(
        &service,
        &explicit_calendar_publication_request,
        &calendar_candidates,
    )
    .await?;
    assert!(
        calendar_excluded.exclusions()[0]
            .reasons()
            .contains(PointInTimeExclusionReason::PublicationAfterCutoff)
    );

    let label_request = request(
        latest,
        as_of,
        None,
        exact(50),
        Some(exact(70)),
        selection_limits,
    )?;
    let before_label_window = macro_candidate(
        "LABEL-PAST",
        "label-past",
        1,
        1,
        exact(50),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let inside_label_window = macro_candidate(
        "LABEL-NOW",
        "label-now",
        2,
        1,
        exact(60),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let after_label_window = macro_candidate(
        "LABEL-FUTURE",
        "label-future",
        3,
        1,
        exact(71),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let label_candidates = [
        before_label_window,
        inside_label_window.clone(),
        after_label_window,
    ];
    let labels = select(&service, &label_request, &label_candidates).await?;
    assert_eq!(labels.records().len(), 1);
    assert_eq!(labels.records()[0].candidate(), &inside_label_window);
    assert_eq!(labels.exclusion_counts().effective_not_after_cutoff(), 1);
    assert_eq!(labels.exclusion_counts().effective_after_label_cutoff(), 1);
    assert!(matches!(
        PointInTimeRequest::try_new(
            latest,
            as_of,
            None,
            exact(70),
            Some(exact(50)),
            selection_limits,
        ),
        Err(PointInTimeError::InvalidLabelWindow)
    ));

    let conflict_left = macro_candidate(
        "CONFLICT",
        "conflict-family",
        20,
        4,
        exact(40),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let conflict_right = macro_candidate(
        "CONFLICT",
        "conflict-family",
        21,
        4,
        exact(40),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_b.clone(),
    )?;
    assert_eq!(conflict_left.family_key()?, conflict_right.family_key()?);
    let conflict_candidates = [conflict_right.clone(), conflict_left.clone()];
    let conflict_error = select_error(&service, &base_request, &conflict_candidates).await?;
    let PointInTimeError::RevisionConflicts { report } = conflict_error else {
        return Err(format!("expected revision conflict, found {conflict_error:?}").into());
    };
    assert_eq!(report.conflict_counts().conflicting_groups(), 1);
    assert_eq!(report.conflict_counts().conflicting_candidates(), 2);
    assert_eq!(report.conflicts()[0].records().len(), 2);
    assert!(
        report.conflicts()[0]
            .records()
            .iter()
            .any(|record| record.candidate().source_manifest() == &manifest_a)
    );
    assert!(
        report.conflicts()[0]
            .records()
            .iter()
            .any(|record| record.candidate().source_manifest() == &manifest_b)
    );
    let reverse_conflict_candidates = [conflict_left.clone(), conflict_right.clone()];
    let reverse_conflict_error =
        select_error(&service, &base_request, &reverse_conflict_candidates).await?;
    let PointInTimeError::RevisionConflicts {
        report: reverse_report,
    } = reverse_conflict_error
    else {
        return Err("reverse conflict did not fail closed".into());
    };
    assert_eq!(report.audit_identity(), reverse_report.audit_identity());

    let second_conflict_left = macro_candidate(
        "CONFLICT-2",
        "conflict-family-2",
        30,
        1,
        exact(40),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_a.clone(),
    )?;
    let second_conflict_right = macro_candidate(
        "CONFLICT-2",
        "conflict-family-2",
        31,
        1,
        exact(40),
        None,
        None,
        AvailabilityEvidence::local_first_observed(timestamp(30)),
        manifest_b,
    )?;
    let conflict_limited_request = request(
        latest,
        as_of,
        None,
        effective_cutoff,
        None,
        limits(8, 8, 1, 8, 1 << 20)?,
    )?;
    let conflict_limit_candidates = [
        conflict_left,
        conflict_right,
        second_conflict_left,
        second_conflict_right,
    ];
    assert!(matches!(
        select_error(
            &service,
            &conflict_limited_request,
            &conflict_limit_candidates,
        )
        .await?,
        PointInTimeError::ConflictLimitExceeded {
            limit: 1,
            observed: 2,
        }
    ));

    let candidate_limit_candidates = [revision_one.clone(), revision_two.clone()];
    assert!(matches!(
        select_error(
            &service,
            &request(
                latest,
                as_of,
                None,
                exact(50),
                None,
                limits(1, 2, 2, 2, 4096)?
            )?,
            &candidate_limit_candidates,
        )
        .await?,
        PointInTimeError::CandidateLimitExceeded {
            limit: 1,
            observed: 2,
        }
    ));
    let family_limit_candidates = [revision_two.clone(), calendar_published.clone()];
    assert!(matches!(
        select_error(
            &service,
            &request(
                latest,
                as_of,
                None,
                exact(50),
                None,
                limits(2, 1, 2, 2, 4096)?
            )?,
            &family_limit_candidates,
        )
        .await?,
        PointInTimeError::FamilyLimitExceeded {
            limit: 1,
            observed: 2,
        }
    ));
    let result_limit_candidates = [revision_two.clone(), calendar_published.clone()];
    assert!(matches!(
        select_error(
            &service,
            &request(
                latest,
                as_of,
                None,
                exact(50),
                None,
                limits(2, 2, 2, 1, 4096)?
            )?,
            &result_limit_candidates,
        )
        .await?,
        PointInTimeError::ResultRowLimitExceeded {
            limit: 1,
            observed: 2,
        }
    ));
    let retained_limit_candidates = [revision_two, calendar_published];
    assert!(matches!(
        select_error(
            &service,
            &request(latest, as_of, None, exact(50), None, limits(2, 2, 2, 2, 1)?)?,
            &retained_limit_candidates,
        )
        .await?,
        PointInTimeError::RetainedBytesExceeded { limit: 1, .. }
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_candidates = [compacted_revision_two.clone()];
    assert!(matches!(
        service
            .select(
                &base_request,
                &cancelled_candidates,
                &cancelled,
                Instant::now() + Duration::from_secs(1),
            )
            .await,
        Err(PointInTimeError::Cancelled)
    ));
    let deadline_candidates = [compacted_revision_two];
    assert!(matches!(
        service
            .select(
                &base_request,
                &deadline_candidates,
                &CancellationToken::new(),
                Instant::now() - Duration::from_millis(1),
            )
            .await,
        Err(PointInTimeError::DeadlineExceeded)
    ));

    Ok(())
}

async fn select<'a>(
    service: &PointInTimeService,
    request: &PointInTimeRequest,
    candidates: &'a [PointInTimeCandidate],
) -> Result<PointInTimeSelection<'a>, Box<dyn Error>> {
    service
        .select(
            request,
            candidates,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .map_err(|error| format!("point-in-time selection failed: {error}").into())
}

async fn select_error<'a>(
    service: &PointInTimeService,
    request: &PointInTimeRequest,
    candidates: &'a [PointInTimeCandidate],
) -> Result<PointInTimeError<'a>, Box<dyn Error>> {
    match service
        .select(
            request,
            candidates,
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
    {
        Ok(_) => Err("selection unexpectedly succeeded".into()),
        Err(error) => Ok(error),
    }
}

fn request(
    policy: PointInTimePolicy,
    as_of: Timestamp,
    publication_cutoff: Option<ResearchTemporalCoordinate>,
    effective_cutoff: ResearchTemporalCoordinate,
    label_cutoff: Option<ResearchTemporalCoordinate>,
    limits: PointInTimeLimits,
) -> Result<PointInTimeRequest, PointInTimeError<'static>> {
    PointInTimeRequest::try_new(
        policy,
        as_of,
        publication_cutoff,
        effective_cutoff,
        label_cutoff,
        limits,
    )
}

fn policy(
    revision_mode: PointInTimeRevisionMode,
) -> Result<PointInTimePolicy, PointInTimeError<'static>> {
    PointInTimePolicy::try_new(NonZeroU32::MIN, revision_mode)
}

fn limits(
    max_candidates: usize,
    max_families: usize,
    max_conflicts: usize,
    max_result_rows: usize,
    max_retained_bytes: usize,
) -> Result<PointInTimeLimits, PointInTimeError<'static>> {
    PointInTimeLimits::try_new(
        max_candidates,
        max_families,
        max_conflicts,
        max_result_rows,
        max_retained_bytes,
    )
}

#[allow(clippy::too_many_arguments)]
fn macro_candidate(
    series: &str,
    source_record: &str,
    value: i64,
    revision: u32,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    superseded: Option<ResearchTemporalCoordinate>,
    availability: AvailabilityEvidence,
    source_manifest: DatasetManifestRef,
) -> Result<PointInTimeCandidate, Box<dyn Error>> {
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("local-research")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(source_record)?,
            source_timestamp: None,
            received_at: timestamp(900),
            ingested_at: timestamp(1_000),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                DigestAlgorithm::Sha256,
                [u8::try_from(value.rem_euclid(251))?; 32],
            )),
            availability,
        })?,
        ResearchTime::try_new_with_coordinates(
            effective,
            published,
            RevisionNumber::new(revision)?,
            superseded,
        )?,
    )?;
    Ok(PointInTimeCandidate::new(
        ResearchObservation::Macro(MacroObservation::new(
            context,
            SourceIdentifier::try_from(series)?,
            Decimal::new(value, 0),
            SourceIdentifier::try_from("index")?,
        )),
        source_manifest,
    ))
}

fn manifest(version: u64, digest: u8) -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        market_squawk_data::DatasetId::try_from("pit-observations")?,
        version,
        market_squawk_data::DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([digest; 32]),
    )?)
}

const fn timestamp(value: i64) -> Timestamp {
    Timestamp::from_unix_nanos(value)
}

const fn exact(value: i64) -> ResearchTemporalCoordinate {
    ResearchTemporalCoordinate::exact(timestamp(value))
}

fn calendar(year: u16, month: u8, day: u8) -> Result<ResearchTemporalCoordinate, Box<dyn Error>> {
    Ok(ResearchTemporalCoordinate::calendar_date(
        CalendarDate::new(year, month, day)?,
    ))
}
