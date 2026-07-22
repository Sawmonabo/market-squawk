use std::error::Error;

use market_squawk::{AppPaths, ProductionBacktestService};
use market_squawk_backtesting::{ExperimentLimits, ExperimentLimitsInput};

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
    )?;

    assert!(paths.artifacts()?.root().join("backtesting/v1").is_dir());
    Ok(())
}
