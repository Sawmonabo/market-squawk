use market_squawk_domain::{
    Currency, DataQuality, DigestAlgorithm, EvidenceDigest, FinancialError, InstrumentId, Money,
    RevisionNumber, Timestamp,
};
use market_squawk_modeling::ProductionFeatureRegistry;
use market_squawk_portfolio::RebalanceTarget;
use rust_decimal::Decimal;
use std::{
    any::TypeId,
    num::{NonZeroU32, NonZeroUsize},
};

use crate::{
    AppendOutcome, AsOfSemantics, CandidateFlag, CandidateId, CandidateInput, ComparisonOperator,
    DecisionActorId, DecisionAuthority, DecisionContentDigest, DecisionContractError,
    DecisionRepository, DecisionRepositoryError, DecisionRepositoryLimits, DecisionText, DossierId,
    GovernedTargetSet, InvestmentTargetSet, InvestmentTargetSetId, NullPolicy, RankingDirection,
    ReferenceMark, SavedScreen, ScreenConstraints, ScreenFeatureBinding, ScreenFeatureObservation,
    ScreenId, ScreenPredicate, ScreenRanking, ScreenRevision, ScreenRun, ScreenRunId,
    TargetAssumption, TargetDecisionContext, TargetEvidence, TargetGovernanceInput,
    TargetInvalidationId, TargetMethod, TargetPriceCases, TargetPriceRange, TargetReview,
    TargetReviewDisposition, TargetReviewId, TargetStatus,
};

fn content_digest(byte: u8) -> Result<DecisionContentDigest, DecisionContractError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]))
}

fn money(amount: i64, currency: &str) -> Result<Money, FinancialError> {
    Ok(Money::new(
        Decimal::new(amount, 2),
        Currency::try_from(currency)?,
    ))
}

fn target(expires_at: Timestamp) -> Result<InvestmentTargetSet, Box<dyn std::error::Error>> {
    let observed_at = Timestamp::from_unix_nanos(10);
    let reference = ReferenceMark::try_new(money(10_000, "USD")?, observed_at, content_digest(1)?)?;
    Ok(InvestmentTargetSet::try_new(
        InvestmentTargetSetId::try_new("target.alpha")?,
        RevisionNumber::new(1)?,
        DossierId::try_new("dossier.alpha")?,
        "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse::<InstrumentId>()?,
        reference,
        TargetPriceCases::try_new(
            money(8_000, "USD")?,
            money(12_000, "USD")?,
            money(16_000, "USD")?,
        )?,
        TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "USD")?)?,
        TargetPriceRange::try_new(money(13_000, "USD")?, money(14_000, "USD")?)?,
        TargetPriceRange::try_new(money(7_000, "USD")?, money(8_000, "USD")?)?,
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(90),
        expires_at,
        content_digest(2)?,
    )?)
}

fn limits() -> Result<DecisionRepositoryLimits, DecisionRepositoryError> {
    DecisionRepositoryLimits::try_new(8, 8, 8, 8, 8, 8, 8)
}

fn governed_target(
    revision: u32,
    supersedes: Option<(RevisionNumber, Timestamp)>,
) -> Result<GovernedTargetSet, Box<dyn std::error::Error>> {
    let mut core = target(Timestamp::from_unix_nanos(120))?;
    if revision != 1 {
        core = InvestmentTargetSet::try_new(
            InvestmentTargetSetId::try_new("target.alpha")?,
            RevisionNumber::new(revision)?,
            DossierId::try_new("dossier.alpha")?,
            "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse::<InstrumentId>()?,
            ReferenceMark::try_new(
                money(10_000, "USD")?,
                Timestamp::from_unix_nanos(10),
                content_digest(1)?,
            )?,
            TargetPriceCases::try_new(
                money(8_000, "USD")?,
                money(12_000, "USD")?,
                money(16_000, "USD")?,
            )?,
            TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "USD")?)?,
            TargetPriceRange::try_new(money(13_000, "USD")?, money(14_000, "USD")?)?,
            TargetPriceRange::try_new(money(7_000, "USD")?, money(8_000, "USD")?)?,
            Timestamp::from_unix_nanos(20 + i64::from(revision)),
            Timestamp::from_unix_nanos(90),
            Timestamp::from_unix_nanos(120),
            content_digest(10 + u8::try_from(revision)?)?,
        )?;
    }
    Ok(GovernedTargetSet::try_new(TargetGovernanceInput {
        target: core,
        add_case: money(10_500, "USD")?,
        method: TargetMethod::ForecastDistribution,
        assumptions: vec![TargetAssumption::new(
            DecisionText::try_new("revenue growth remains positive")?,
            content_digest(20)?,
        )],
        decision_context: TargetDecisionContext::new(DossierId::try_new("dossier.alpha")?, None),
        effective_at: Timestamp::from_unix_nanos(25 + i64::from(revision)),
        review_due_at: Timestamp::from_unix_nanos(80),
        supersedes,
        thesis: DecisionText::try_new("durable operating leverage")?,
        risks: vec![DecisionText::try_new("demand contraction")?],
        invalidation_conditions: vec![DecisionText::try_new("guidance withdrawn")?],
        evidence: TargetEvidence::new(Some(content_digest(21)?), None),
        mark_quality: DataQuality::DirectVerified,
        author: DecisionActorId::try_new("author.alpha")?,
        ruleset_version: NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
    })?)
}

#[test]
fn target_set_rejects_mixed_currency() -> Result<(), Box<dyn std::error::Error>> {
    let result = TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "EUR")?);

    assert_eq!(result, Err(DecisionContractError::CurrencyMismatch));
    Ok(())
}

#[test]
fn activation_review_rejects_expired_target() -> Result<(), Box<dyn std::error::Error>> {
    let target = target(Timestamp::from_unix_nanos(100))?;

    let result = TargetReview::try_new(
        TargetReviewId::try_new("review.alpha")?,
        &target,
        DecisionActorId::try_new("reviewer.alpha")?,
        Timestamp::from_unix_nanos(100),
        TargetReviewDisposition::Activate,
        content_digest(3)?,
    );

    assert_eq!(result, Err(DecisionContractError::ExpiredActivation));
    Ok(())
}

#[test]
fn target_set_rejects_reversed_case_order() -> Result<(), Box<dyn std::error::Error>> {
    let result = TargetPriceCases::try_new(
        money(13_000, "USD")?,
        money(12_000, "USD")?,
        money(16_000, "USD")?,
    );

    assert_eq!(result, Err(DecisionContractError::InvalidPriceOrder));
    Ok(())
}

#[test]
fn screen_run_binds_exact_pit_inputs_and_rejects_semantic_substitution()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let metadata = registry
        .feature_registry()
        .entries()
        .find(|metadata| {
            metadata.is_point_in_time_compatible()
                && metadata.output_type()
                    == market_squawk_analytics::FeatureOutputType::StatisticalF64
        })
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let binding = ScreenFeatureBinding::new(metadata.key().clone(), metadata.semantic_digest());
    let saved = SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.quality")?,
            RevisionNumber::new(1)?,
        ),
        content_digest(30)?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            binding.clone(),
            ComparisonOperator::GreaterThanOrEqual,
            market_squawk_analytics::StatisticalF64::try_new(0.5)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(binding.clone(), RankingDirection::Descending),
        NonZeroUsize::new(2).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            market_squawk_analytics::StatisticalF64::try_new(0.8)?,
            market_squawk_analytics::StatisticalF64::try_new(1_000.0)?,
            vec![DataQuality::DirectVerified],
        )?,
        registry.feature_registry(),
    )?;
    let mut authority = DecisionAuthority::new(DecisionRepository::try_new(limits()?)?);
    authority.save_screen(None, saved.clone())?;
    let dataset = content_digest(31)?;
    let universe = saved.universe_identity();
    let run = ScreenRun::try_new(
        ScreenRunId::try_new("run.quality.1")?,
        saved.revision().clone(),
        Timestamp::from_unix_nanos(50),
        dataset,
        universe,
        vec![binding.clone()],
    )?;
    let input = CandidateInput::try_new(
        CandidateId::try_new("candidate.alpha")?,
        "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse()?,
        vec![ScreenFeatureObservation::new(
            binding.clone(),
            Some(market_squawk_analytics::StatisticalF64::try_new(0.75)?),
        )],
        market_squawk_analytics::StatisticalF64::try_new(0.95)?,
        market_squawk_analytics::StatisticalF64::try_new(10_000.0)?,
        DataQuality::DirectVerified,
        None,
        vec![CandidateFlag::ModelDependent],
        content_digest(32)?,
    )?;
    let execution = authority.run_screen(run, vec![input], Timestamp::from_unix_nanos(51))?;

    assert_eq!(execution.run().dataset_identity(), dataset);
    assert_eq!(execution.run().universe_identity(), universe);
    assert_eq!(
        execution.run().feature_bindings(),
        std::slice::from_ref(&binding)
    );
    assert_eq!(execution.candidates().len(), 1);

    let wrong_metadata = registry
        .feature_registry()
        .entries()
        .find(|candidate| candidate.key() != metadata.key())
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let wrong_binding = ScreenFeatureBinding::new(
        wrong_metadata.key().clone(),
        wrong_metadata.semantic_digest(),
    );
    let substituted = ScreenRun::try_new(
        ScreenRunId::try_new("run.quality.2")?,
        saved.revision().clone(),
        Timestamp::from_unix_nanos(52),
        content_digest(34)?,
        universe,
        vec![wrong_binding],
    )?;
    assert_eq!(
        authority.run_screen(substituted, Vec::new(), Timestamp::from_unix_nanos(53)),
        Err(DecisionRepositoryError::EvidenceMismatch)
    );
    Ok(())
}

#[test]
fn saved_screen_rejects_sql_like_unknown_features_without_a_formula_surface()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let injected_key = market_squawk_analytics::FeatureKey::try_new(
        "select.from.securities",
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
    )?;
    let injected = ScreenFeatureBinding::new(
        injected_key,
        registry
            .feature_registry()
            .entries()
            .next()
            .ok_or(DecisionContractError::UnknownScreenFeature)?
            .semantic_digest(),
    );
    let result = SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.injected")?,
            RevisionNumber::new(1)?,
        ),
        content_digest(40)?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            injected.clone(),
            ComparisonOperator::Equal,
            market_squawk_analytics::StatisticalF64::try_new(1.0)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(injected, RankingDirection::Descending),
        NonZeroUsize::new(1).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            market_squawk_analytics::StatisticalF64::try_new(0.5)?,
            market_squawk_analytics::StatisticalF64::try_new(0.0)?,
            vec![DataQuality::DirectVerified],
        )?,
        registry.feature_registry(),
    );

    assert_eq!(result, Err(DecisionContractError::UnknownScreenFeature));
    assert!(
        market_squawk_analytics::FeatureKey::try_new(
            "price * 2; select *",
            NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn target_revision_history_is_append_only_and_not_a_rebalance_target()
-> Result<(), Box<dyn std::error::Error>> {
    let mut repository = DecisionRepository::try_new(limits()?)?;
    let first = governed_target(1, None)?;
    let second = governed_target(
        2,
        Some((RevisionNumber::new(1)?, Timestamp::from_unix_nanos(27))),
    )?;
    repository.append_target(None, first.clone())?;
    repository.append_target(Some(RevisionNumber::new(1)?), second.clone())?;

    let revisions = repository
        .target_revisions(first.target().id())
        .collect::<Vec<_>>();
    assert_eq!(revisions, vec![&first, &second]);
    assert_eq!(
        repository.target_status(first.target().id(), RevisionNumber::new(1)?)?,
        TargetStatus::Superseded
    );
    assert_ne!(
        TypeId::of::<GovernedTargetSet>(),
        TypeId::of::<RebalanceTarget>()
    );
    Ok(())
}

#[test]
fn every_invalidator_appends_idempotent_needs_review_without_replacing_approval()
-> Result<(), Box<dyn std::error::Error>> {
    let mut repository = DecisionRepository::try_new(limits()?)?;
    let governed = governed_target(1, None)?;
    let unchanged = governed.clone();
    repository.append_target(None, governed.clone())?;
    let review = TargetReview::try_new(
        TargetReviewId::try_new("review.activate")?,
        governed.target(),
        DecisionActorId::try_new("reviewer.alpha")?,
        Timestamp::from_unix_nanos(60),
        TargetReviewDisposition::Activate,
        content_digest(50)?,
    )?;
    repository.append_review(review.clone())?;
    assert_eq!(
        repository.target_status(governed.target().id(), RevisionNumber::new(1)?)?,
        TargetStatus::Active
    );

    for (index, kind) in crate::InvalidationKind::ALL.into_iter().enumerate() {
        let invalidation = crate::TargetInvalidation::try_new(
            TargetInvalidationId::try_new(format!("invalidate.{index}"))?,
            governed.target(),
            kind,
            Timestamp::from_unix_nanos(70 + i64::try_from(index)?),
            content_digest(60 + u8::try_from(index)?)?,
        )?;
        assert_eq!(
            repository.append_invalidation(invalidation.clone())?,
            AppendOutcome::Appended
        );
        assert_eq!(
            repository.append_invalidation(invalidation)?,
            AppendOutcome::AlreadyPresent
        );
    }

    assert_eq!(
        repository.target_status(governed.target().id(), RevisionNumber::new(1)?)?,
        TargetStatus::NeedsReview
    );
    assert_eq!(
        repository
            .target_revisions(governed.target().id())
            .collect::<Vec<_>>(),
        vec![&unchanged]
    );
    assert_eq!(
        repository
            .reviews(governed.target().id(), RevisionNumber::new(1)?)
            .collect::<Vec<_>>(),
        vec![&review]
    );
    assert_eq!(
        repository
            .invalidations(governed.target().id(), RevisionNumber::new(1)?)
            .count(),
        crate::InvalidationKind::ALL.len()
    );
    Ok(())
}
