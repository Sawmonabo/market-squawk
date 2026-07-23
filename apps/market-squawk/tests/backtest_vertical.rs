use std::error::Error;

use market_squawk::{
    AppPaths, PinnedBacktestInput, ProductionBacktestService, ProductionBacktestServiceError,
};
use market_squawk_backtesting::{
    BacktestOutcome, BacktestStrategyRegistry, ExperimentLimits, ExperimentLimitsInput,
};
use market_squawk_data::PinnedInstrumentDefinitions;
use market_squawk_domain::SourceIdentifier;
use tokio_util::sync::CancellationToken;

#[test]
fn production_backtest_inventory_is_confined_to_the_controlled_artifact_root()
-> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    let paths = AppPaths::prepare(temporary.path().join("market-squawk"))?;
    let _service = ProductionBacktestService::initialize(
        &paths,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 8,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 8,
        })?,
        BacktestStrategyRegistry::try_new(Vec::new())?,
    )?;

    let run_boundary: fn(
        &ProductionBacktestService,
        PinnedBacktestInput,
        &SourceIdentifier,
        &CancellationToken,
    ) -> Result<BacktestOutcome, ProductionBacktestServiceError> = ProductionBacktestService::run;
    let _ = run_boundary;
    let input_contract: fn(PinnedBacktestInput) -> PinnedInstrumentDefinitions =
        |input| input.instrument_definitions;
    let _ = input_contract;
    assert!(paths.artifacts()?.root().join("backtesting/v1").is_dir());
    Ok(())
}
