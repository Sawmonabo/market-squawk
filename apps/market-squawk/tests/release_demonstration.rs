use std::collections::BTreeMap;
use std::ffi::OsString;

use market_squawk::cli::{ReleaseCommand, ReleaseDemonstrateArguments, ReleaseRepositoryArguments};
use market_squawk::release::execute_release_command;
use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};

#[tokio::test]
async fn usable_release_vertical_requires_explicit_offline_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let config = AppConfig::load(ConfigSources::new(
        None,
        &BTreeMap::<OsString, OsString>::new(),
        ConfigOverrides {
            data_dir: Some(temporary.path().join("data")),
            ..ConfigOverrides::default()
        },
    ))?;
    let error = match execute_release_command(
        config,
        ReleaseCommand::Demonstrate(ReleaseDemonstrateArguments {
            repository: ReleaseRepositoryArguments {
                head: None,
                tree: None,
            },
            offline: false,
            provider_evidence: temporary.path().join("providers"),
            python_evidence: temporary.path().join("python/market-squawk-release.json"),
            output: temporary.path().join("demo.json"),
        }),
    )
    .await
    {
        Ok(_value) => return Err("release demonstration accepted absent offline admission".into()),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("release demonstration requires --offline")
    );
    Ok(())
}
