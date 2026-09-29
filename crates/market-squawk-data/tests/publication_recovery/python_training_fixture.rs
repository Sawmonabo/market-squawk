//! Verification-only export through the same sealed producer exercised by the parent test.
//! This is compiled only into the existing integration-test executable, never the product.

use super::*;
use std::{fs::OpenOptions, io::Write, path::Path};

pub(super) fn prices(index: usize) -> Result<[i64; 3], Box<dyn Error>> {
    let forward = [5_i64, 25, 35, 65, 75, 105]
        .get(index)
        .ok_or("fixture example limit")?;
    let previous = 1_000_000_i64;
    let current = previous + i64::try_from(index)? * 10_000;
    let terminal = current
        .checked_mul(1_000 + forward)
        .ok_or("fixture price overflow")?
        / 1_000;
    Ok([previous, current, terminal])
}

pub(super) async fn publish(root: &Path) -> TestResult {
    // An explicitly requested empty test root is the entire write capability. Existing user or
    // release state is never opened for fixture mutation, restored over, or copied by SQL.
    if !root.is_absolute()
        || !std::fs::symlink_metadata(root)?.is_dir()
        || std::fs::read_dir(root)?.next().is_some()
    {
        return Err("dataset fixture requires an empty absolute directory".into());
    }
    let paths = LocalPaths::prepare(root)?;
    let mut values = Vec::with_capacity(18);
    for index in 0..6 {
        let at = 100 * (i64::try_from(index)? + 1);
        let [previous, current, terminal] = prices(index)?;
        values.extend([(at - 20, previous), (at - 10, current), (at, terminal)]);
    }
    let market = closed_price_return_market_bar_fixture_for_values(
        &values,
        Timestamp::from_unix_nanos(610),
    )?;
    let (service, publisher, membership, market_bars) = initialized_service_with_universe_fixture(
        &paths,
        test_catalog_config(paths.catalog()?.clone())?,
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        market,
        true,
    )
    .await?;
    let research_limits = ResearchUseLimits::try_new(
        8,
        32,
        32,
        8,
        1024 * 1024,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    let request = closed_price_return_request_for_fixture(
        membership.manifest().clone(),
        market_bars.manifest().clone(),
        dataset_membership_instrument()?,
        research_limits,
        false,
        false,
        true,
    )?;
    let cancellation = CancellationToken::new();
    let dataset = service
        .dataset_builder()
        .build(request.clone(), cancellation.clone())
        .await
        .inspect_err(|_| eprintln!("dataset fixture stage=build"))?;
    let contract =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1;
    let wall = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let publication = publisher.publish(
        &service,
        contract,
        &request,
        &dataset,
        closed_price_return_proof(
            &request,
            membership.manifest().clone(),
            wall.checked_sub_nanos(1_000_000_000)?,
            wall.checked_add_nanos(120_000_000_000)?,
            96,
        )?,
        &cancellation,
    )
    .inspect_err(|_| eprintln!("dataset fixture stage=publish"))?;
    assert_eq!(
        publication.disposition(),
        FeatureDatasetProductionPublicationDisposition::Published
    );
    assert_eq!(dataset.split_counts().train_examples(), 2);
    assert_eq!(dataset.split_counts().validation_examples(), 2);
    assert_eq!(dataset.split_counts().test_examples(), 2);
    let export = dataset.python_export()?;
    // Read through the exact same production admission/receipt/row verifier used by Python.
    // The descriptor alone is not evidence that training admission succeeded.
    market_squawk_data::verify_python_dataset(
        root,
        export.content_hash(),
        contract,
        Timestamp::from_unix_nanos(700),
        market_squawk_data::PythonDatasetVerificationLimits::try_new(128, 64 * 1024 * 1024)?,
        Instant::now() + Duration::from_secs(30),
        &cancellation,
    )
    .inspect_err(|_| eprintln!("dataset fixture stage=verify"))?;
    write_new(&root.join("fixture-export.json"), export.bytes())?;
    write_new(
        &root.join("fixture-receipt.json"),
        publication.receipt().canonical_json(),
    )?;
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> TestResult {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
