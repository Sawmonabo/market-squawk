use std::error::Error;
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;

use market_squawk_data::{
    AdjustmentConflict, AdjustmentRatio, AdjustmentStep, CorporateActionAdjustment,
    CorporateActionError, CorporateActionExclusionReason, CorporateActionLimits,
    CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord, DatasetId,
    DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{
    AvailabilityEvidence, CorporateActionKind, CorporateActionObservation, Currency, DataQuality,
    DigestAlgorithm, EvidenceDigest, InstrumentId, MergerConsideration, Money, PayloadReference,
    ResearchContext, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceId, SourceIdentifier, Timestamp, VenueId, VenueSymbol,
};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn Error>>;

fn manifest(marker: u8) -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("corporate-actions")?,
        u64::from(marker),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker; 32]),
    )?)
}

fn record(
    marker: u8,
    effective_at: i64,
    availability: AvailabilityEvidence,
    action: CorporateActionKind,
) -> Result<CorporateActionRecord, Box<dyn Error>> {
    let source_identifier = SourceIdentifier::try_from(format!("corporate-action-{marker}"))?;
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("official-corporate-actions")?,
            instrument_id: Some(InstrumentId::from_str(
                "0187f5f1-6fc2-7fa2-bf05-2ce5354c55c1",
            )?),
            venue_id: Some(VenueId::try_from("XNYS")?),
            source_identifier: source_identifier.clone(),
            source_timestamp: Some(Timestamp::from_unix_nanos(effective_at)),
            received_at: Timestamp::from_unix_nanos(150),
            ingested_at: Timestamp::from_unix_nanos(200),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(source_identifier),
            availability,
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(effective_at),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    Ok(CorporateActionRecord::new(
        CorporateActionObservation::new(context, action)?,
        manifest(marker)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]),
    ))
}

fn evidenced(at: i64, marker: u8) -> Result<AvailabilityEvidence, Box<dyn Error>> {
    Ok(AvailabilityEvidence::evidenced(
        Timestamp::from_unix_nanos(at),
        SourceIdentifier::try_from(format!("availability-{marker}"))?,
    ))
}

#[test]
fn adjustment_plan_is_time_correct_exact_deterministic_and_bounded() -> TestResult {
    let one = NonZeroU32::MIN;
    let two = NonZeroU32::new(2).ok_or("two")?;
    let three = NonZeroU32::new(3).ok_or("three")?;
    let usd = Currency::try_from("USD")?;
    let dividend = Money::new(Decimal::new(125, 2), usd);
    let capital_return = Money::new(Decimal::new(75, 2), usd);
    let distributed = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c2")?;
    let successor = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c3")?;
    let incomplete_successor = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55c4")?;
    let actions = vec![
        record(
            1,
            80,
            evidenced(90, 1)?,
            CorporateActionKind::Split {
                numerator: two,
                denominator: one,
            },
        )?,
        record(
            2,
            70,
            evidenced(95, 2)?,
            CorporateActionKind::CashDividend { amount: dividend },
        )?,
        record(
            3,
            75,
            evidenced(95, 3)?,
            CorporateActionKind::ReturnOfCapital {
                amount: capital_return,
            },
        )?,
        record(
            4,
            60,
            evidenced(90, 4)?,
            CorporateActionKind::Spinoff {
                distributed_instrument: distributed,
                numerator: one,
                denominator: three,
            },
        )?,
        record(
            5,
            65,
            evidenced(90, 5)?,
            CorporateActionKind::Merger {
                successor,
                consideration: MergerConsideration::Mixed {
                    numerator: three,
                    denominator: two,
                    cash: Money::new(Decimal::new(50, 2), usd),
                },
            },
        )?,
        record(
            6,
            66,
            evidenced(90, 6)?,
            CorporateActionKind::Merger {
                successor: incomplete_successor,
                consideration: MergerConsideration::Unspecified,
            },
        )?,
        record(7, 100, evidenced(100, 7)?, CorporateActionKind::Delisting)?,
        record(
            8,
            55,
            evidenced(85, 8)?,
            CorporateActionKind::SymbolChange {
                venue_id: VenueId::try_from("XNYS")?,
                previous: VenueSymbol::try_from("OLD")?,
                current: VenueSymbol::try_from("NEW")?,
            },
        )?,
        record(
            9,
            50,
            evidenced(101, 9)?,
            CorporateActionKind::CashDividend {
                amount: Money::new(Decimal::new(999, 2), usd),
            },
        )?,
        record(
            10,
            101,
            evidenced(90, 10)?,
            CorporateActionKind::Split {
                numerator: three,
                denominator: two,
            },
        )?,
        record(
            11,
            40,
            AvailabilityEvidence::unknown(),
            CorporateActionKind::Delisting,
        )?,
    ];
    let original = actions.clone();
    let knowledge_cutoff = Timestamp::from_unix_nanos(100);
    let valuation_cutoff = Timestamp::from_unix_nanos(100);
    let limits = CorporateActionLimits::try_new(
        NonZeroUsize::new(32).ok_or("action limit")?,
        NonZeroUsize::new(1024 * 1024).ok_or("retained byte limit")?,
    )?;
    let policy = |adjustment| CorporateActionPolicy::new(adjustment, NonZeroU32::MIN);

    let raw = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::Raw),
        knowledge_cutoff,
        valuation_cutoff,
        actions.clone(),
        limits,
    )?;
    assert_eq!(raw.admitted().len(), 8);
    assert!(raw.steps().is_empty());
    assert!(raw.conflicts().is_empty());

    let split_adjusted = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::SplitAdjusted),
        knowledge_cutoff,
        valuation_cutoff,
        actions.clone(),
        limits,
    )?;
    assert_eq!(split_adjusted.steps().len(), 1);
    assert!(matches!(
        &split_adjusted.steps()[0],
        AdjustmentStep::Split {
            price_factor,
            quantity_factor,
            ..
        } if *price_factor == AdjustmentRatio::new(one, two)
            && *quantity_factor == AdjustmentRatio::new(two, one)
    ));

    let total_return = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::TotalReturn),
        knowledge_cutoff,
        valuation_cutoff,
        actions.clone(),
        limits,
    )?;
    assert_eq!(total_return.admitted().len(), 8);
    assert_eq!(total_return.steps().len(), 7);
    assert_eq!(total_return.exclusions().len(), 3);
    assert_eq!(total_return.conflicts().len(), 1);
    assert!(total_return.retained_bytes() <= limits.max_retained_bytes().get());
    assert!(total_return.steps().iter().any(|step| matches!(
        step,
        AdjustmentStep::CashDividend { amount, .. } if *amount == dividend
    )));
    assert!(total_return.steps().iter().any(|step| matches!(
        step,
        AdjustmentStep::ReturnOfCapital { amount, .. } if *amount == capital_return
    )));
    assert!(total_return.steps().iter().any(|step| matches!(
        step,
        AdjustmentStep::Spinoff {
            distributed_instrument,
            distribution_ratio,
            ..
        } if *distributed_instrument == distributed
            && *distribution_ratio == AdjustmentRatio::new(one, three)
    )));
    assert!(total_return.steps().iter().any(|step| matches!(
        step,
        AdjustmentStep::Merger {
            successor: observed,
            consideration: MergerConsideration::Mixed { cash, .. },
            ..
        } if *observed == successor && *cash == Money::new(Decimal::new(50, 2), usd)
    )));
    assert!(
        total_return
            .steps()
            .iter()
            .any(|step| matches!(step, AdjustmentStep::Delisting { .. }))
    );
    assert!(
        total_return
            .steps()
            .iter()
            .any(|step| matches!(step, AdjustmentStep::SymbolChange { .. }))
    );
    assert!(matches!(
        total_return.conflicts()[0],
        AdjustmentConflict::IncompleteMergerTerms {
            successor: observed,
            ..
        } if observed == incomplete_successor
    ));
    assert!(total_return.exclusions().iter().any(|excluded| {
        excluded.reason() == CorporateActionExclusionReason::FutureAvailability
    }));
    assert!(total_return.exclusions().iter().any(|excluded| {
        excluded.reason() == CorporateActionExclusionReason::FutureEffectiveTime
    }));
    assert!(total_return.exclusions().iter().any(|excluded| {
        excluded.reason() == CorporateActionExclusionReason::UnknownAvailability
    }));

    let retained_split = total_return
        .admitted()
        .iter()
        .find(|candidate| {
            matches!(
                candidate.observation().action(),
                CorporateActionKind::Split { .. }
            )
        })
        .ok_or("missing retained split")?;
    let source_split = original
        .iter()
        .find(|candidate| {
            matches!(
                candidate.observation().action(),
                CorporateActionKind::Split { .. }
            )
        })
        .ok_or("missing source split")?;
    assert_eq!(retained_split, source_split);
    assert_eq!(
        retained_split.source_manifest(),
        source_split.source_manifest()
    );
    assert_eq!(
        retained_split.evidence_digest(),
        source_split.evidence_digest()
    );

    let mut reversed = actions.clone();
    reversed.reverse();
    let reordered = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::TotalReturn),
        knowledge_cutoff,
        valuation_cutoff,
        reversed,
        limits,
    )?;
    assert_eq!(total_return.content_hash(), reordered.content_hash());
    assert_eq!(total_return.audit_hash(), reordered.audit_hash());
    assert_eq!(total_return.admitted(), reordered.admitted());
    assert_eq!(total_return.exclusions(), reordered.exclusions());

    let future_perturbation = record(
        12,
        30,
        evidenced(150, 12)?,
        CorporateActionKind::CashDividend {
            amount: Money::new(Decimal::new(1_000_000, 2), usd),
        },
    )?;
    let mut perturbed = actions.clone();
    perturbed.push(future_perturbation);
    let with_future = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::TotalReturn),
        knowledge_cutoff,
        valuation_cutoff,
        perturbed,
        limits,
    )?;
    assert_eq!(total_return.content_hash(), with_future.content_hash());
    assert_ne!(total_return.audit_hash(), with_future.audit_hash());

    let source_schema = retained_split.source_manifest().schema();
    let altered_schema =
        DatasetSchemaRef::try_new(source_schema.name(), source_schema.version(), [0xa5; 32])?;
    let altered_manifest = DatasetManifestRef::try_new_with_schema(
        retained_split.source_manifest().dataset_id().clone(),
        retained_split.source_manifest().manifest_version(),
        altered_schema,
        retained_split.source_manifest().content_hash(),
    )?;
    let altered_lineage = CorporateActionRecord::new(
        retained_split.observation().clone(),
        altered_manifest,
        retained_split.evidence_digest(),
    );
    let baseline_single = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::TotalReturn),
        knowledge_cutoff,
        valuation_cutoff,
        vec![retained_split.clone()],
        limits,
    )?;
    let altered_single = CorporateActionPlan::try_build(
        policy(CorporateActionAdjustment::TotalReturn),
        knowledge_cutoff,
        valuation_cutoff,
        vec![altered_lineage],
        limits,
    )?;
    assert_ne!(
        baseline_single.content_hash(),
        altered_single.content_hash()
    );

    assert!(matches!(
        CorporateActionPlan::try_build(
            policy(CorporateActionAdjustment::Raw),
            knowledge_cutoff,
            valuation_cutoff,
            actions.iter().take(2).cloned().collect(),
            CorporateActionLimits::try_new(
                NonZeroUsize::MIN,
                NonZeroUsize::new(1024 * 1024).ok_or("byte limit")?,
            )?,
        ),
        Err(CorporateActionError::ActionLimitExceeded {
            limit: 1,
            observed: 2,
        })
    ));
    assert!(matches!(
        CorporateActionPlan::try_build(
            policy(CorporateActionAdjustment::Raw),
            knowledge_cutoff,
            valuation_cutoff,
            vec![actions[0].clone()],
            CorporateActionLimits::try_new(NonZeroUsize::MIN, NonZeroUsize::MIN)?,
        ),
        Err(CorporateActionError::RetainedByteLimitExceeded { limit: 1, .. })
    ));

    let mut capacity_backed_symbol = String::with_capacity(8 * 1024);
    capacity_backed_symbol.push_str("CAPACITY-RETAINED");
    let capacity_backed_action = record(
        13,
        90,
        evidenced(90, 13)?,
        CorporateActionKind::SymbolChange {
            venue_id: VenueId::try_from("XNYS")?,
            previous: VenueSymbol::try_from(capacity_backed_symbol)?,
            current: VenueSymbol::try_from("CURRENT")?,
        },
    )?;
    assert!(matches!(
        CorporateActionPlan::try_build(
            policy(CorporateActionAdjustment::Raw),
            knowledge_cutoff,
            valuation_cutoff,
            vec![capacity_backed_action],
            CorporateActionLimits::try_new(
                NonZeroUsize::MIN,
                NonZeroUsize::new(4 * 1024).ok_or("capacity byte limit")?,
            )?,
        ),
        Err(CorporateActionError::RetainedByteLimitExceeded { limit: 4096, .. })
    ));
    assert!(matches!(
        CorporateActionPlan::try_build(
            policy(CorporateActionAdjustment::Raw),
            knowledge_cutoff,
            valuation_cutoff,
            Vec::new(),
            CorporateActionLimits::try_new(NonZeroUsize::MIN, NonZeroUsize::MIN)?,
        ),
        Err(CorporateActionError::RetainedByteLimitExceeded { limit: 1, .. })
    ));
    Ok(())
}
