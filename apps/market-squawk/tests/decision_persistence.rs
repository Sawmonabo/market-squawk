use std::num::NonZeroUsize;

use market_squawk::application::decision::{DecisionApplication, DecisionApplicationError};
use market_squawk_analytics::{FeatureOutputType, StatisticalF64};
use market_squawk_decisions::{
    AppendOutcome, AsOfSemantics, ComparisonOperator, DecisionContentDigest, DecisionContractError,
    DecisionRepositoryLimits, NullPolicy, RankingDirection, SavedScreen, ScreenConstraints,
    ScreenFeatureBinding, ScreenId, ScreenPredicate, ScreenRanking, ScreenRevision,
};
use market_squawk_domain::{DataQuality, DigestAlgorithm, EvidenceDigest, RevisionNumber};
use market_squawk_modeling::ProductionFeatureRegistry;
use market_squawk_platform::LocalPaths;
use rusqlite::Connection;

#[test]
fn decision_append_is_durable_idempotent_and_recovers_under_one_writer_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let location = paths.control_root()?.decision_database_location();
    let limits = DecisionRepositoryLimits::try_new(8, 8, 8, 8, 8, 8, 8)?;
    let screen = saved_screen()?;

    let application = DecisionApplication::open(location.clone(), limits)?;
    assert_eq!(
        application.save_screen(None, screen.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        application.save_screen(None, screen.clone())?,
        AppendOutcome::AlreadyPresent
    );
    assert!(matches!(
        DecisionApplication::open(location.clone(), limits),
        Err(DecisionApplicationError::Persistence)
    ));
    assert_eq!(record_count(location.path())?, 1);

    drop(application);
    let recovered = DecisionApplication::open(location.clone(), limits)?;
    assert_eq!(
        recovered.get_screen(screen.revision().id(), screen.revision().revision())?,
        screen
    );
    assert_eq!(
        recovered.save_screen(None, screen)?,
        AppendOutcome::AlreadyPresent
    );
    assert_eq!(record_count(location.path())?, 1);
    Ok(())
}

fn saved_screen() -> Result<SavedScreen, Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let metadata = registry
        .feature_registry()
        .entries()
        .find(|metadata| {
            metadata.is_point_in_time_compatible()
                && metadata.output_type() == FeatureOutputType::StatisticalF64
        })
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let binding = ScreenFeatureBinding::new(metadata.key().clone(), metadata.semantic_digest());
    Ok(SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.restart-proof")?,
            RevisionNumber::new(1)?,
        ),
        DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [41; 32]))?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            binding.clone(),
            ComparisonOperator::GreaterThanOrEqual,
            StatisticalF64::try_new(0.5)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(binding, RankingDirection::Descending),
        NonZeroUsize::new(4).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            StatisticalF64::try_new(0.8)?,
            StatisticalF64::try_new(1_000.0)?,
            vec![DataQuality::DirectVerified],
        )?,
        registry.feature_registry(),
    )?)
}

fn record_count(path: &std::path::Path) -> rusqlite::Result<i64> {
    Connection::open(path)?.query_row("SELECT COUNT(*) FROM decision_records", [], |row| {
        row.get(0)
    })
}
