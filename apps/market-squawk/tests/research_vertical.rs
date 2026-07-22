use std::error::Error;
use std::time::Duration;

use market_squawk::{ResearchService, ResearchServiceError};
use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
use market_squawk_platform::LocalPaths;

#[test]
fn research_service_reopens_the_exact_local_catalog_and_artifact_authority()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let objects = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;

    drop(ResearchService::initialize(
        &paths,
        catalog.clone(),
        8,
        objects,
    )?);
    let reopened = ResearchService::open(&paths, catalog, 8, objects);
    assert!(!matches!(
        reopened,
        Err(ResearchServiceError::Ingest(
            market_squawk_data::IngestError::CatalogCompositionMismatch
        ))
    ));
    drop(reopened?);
    Ok(())
}
