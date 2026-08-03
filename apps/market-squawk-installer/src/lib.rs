//! Verified, versioned Market Squawk installation lifecycle.

mod archive;
mod command;
mod contracts;
mod lifecycle;
mod manifest;
mod platform;
mod service_registration;
mod store;
mod update_metadata;

pub use self::archive::ArchiveError;
pub use self::command::{CommandError, run_cli, update_from_channel};
pub use self::contracts::{
    InstallReceipt, InstallRequest, InstallStatus, MutableDataClass, ProgramInstallSnapshot,
    RepairRequest, RollbackRequest, UninstallReceipt, UninstallRequest, UpdateRequest,
};
pub use self::lifecycle::{
    InstallError, active_program_path, active_release_root,
    active_release_root_for_installed_program, install, installation_root_for_installed_program,
    program_install_snapshot, repair, rollback, stable_program_path, status, uninstall, update,
};
pub use self::manifest::{
    AdmittedRelease, ComponentRole, MAXIMUM_MANIFEST_BYTES, ManifestError, ReleaseManifest,
};
pub use self::platform::{
    NativeTrustMode, PlatformError, ProgramName, SupportedTarget, default_install_root,
    default_installation_data_root,
};
pub use self::service_registration::{
    InstalledServiceStatus, RestartInstalledServiceRequest, ServiceRegistrationError,
    installed_service_status, restart_installed_service, verify_installed_service,
};
pub use self::store::StoreError;
pub use self::update_metadata::{
    PendingTrustedUpdate, SuppliedMetadata, SuppliedTarget, TargetSource, TrustedRoot,
    TrustedTarget, TrustedUpdateReceipt, TrustedUpdateStore, UpdateMetadataError,
};

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;
    use std::fs::{self, File};
    use std::io::{Read as _, Write as _};
    use std::path::{Path, PathBuf};

    use semver::Version;
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    use super::{
        ComponentRole, InstallError, InstallRequest, MutableDataClass, ProgramName, RepairRequest,
        RollbackRequest, StoreError, SupportedTarget, UninstallRequest, UpdateRequest,
        active_release_root, install, program_install_snapshot, repair, rollback, status,
        uninstall, update,
    };
    #[cfg(unix)]
    use super::{
        active_release_root_for_installed_program, installation_root_for_installed_program,
        stable_program_path,
    };

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    #[cfg(windows)]
    fn test_directory() -> TestResult<TempDir> {
        let local_app_data = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Windows local application-data directory is unavailable",
            )
        })?;
        let temporary = tempfile::Builder::new()
            .prefix("market-squawk-installer-")
            .tempdir_in(local_app_data)?;
        crate::store::secure_test_store_parent(temporary.path())?;
        Ok(temporary)
    }

    #[cfg(not(windows))]
    fn test_directory() -> TestResult<TempDir> {
        Ok(TempDir::new()?)
    }

    #[test]
    fn rejects_unsafe_unlisted_or_mismatched_bundle_before_activation() -> TestResult {
        for defect in [
            BundleDefect::ParentTraversal,
            BundleDefect::UnlistedEntry,
            BundleDefect::DigestMismatch,
            BundleDefect::MisplacedRequiredRole,
        ] {
            let temporary = test_directory()?;
            let fixture = BundleFixture::create(temporary.path(), "0.1.0", defect)?;
            let result = InstallRequest::from_local(
                temporary.path().join("program"),
                &fixture.manifest,
                &fixture.bundle,
            )
            .and_then(install);

            assert!(result.is_err());
            assert!(!temporary.path().join("program/installation.json").exists());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn install_rejects_a_path_replaceable_by_other_accounts() -> TestResult {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = test_directory()?;
        let fixture = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        let shared = temporary.path().join("shared");
        let private = shared.join("private");
        fs::create_dir_all(&private)?;
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o711))?;
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o777))?;
        fs::set_permissions(&private, fs::Permissions::from_mode(0o700))?;
        let result = install(InstallRequest::from_local(
            private.join("program"),
            &fixture.manifest,
            &fixture.bundle,
        )?);
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700))?;

        assert!(matches!(
            result,
            Err(InstallError::Store(crate::StoreError::UnsafeRoot))
        ));
        Ok(())
    }

    #[test]
    fn activation_and_rollback_switch_complete_versions() -> TestResult {
        let temporary = test_directory()?;
        let root = temporary.path().join("program");
        let first = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &first.manifest,
            &first.bundle,
        )?)?;
        #[cfg(unix)]
        {
            let stable_cli = stable_program_path(&root, ProgramName::Cli)?;
            let stable_service = stable_program_path(&root, ProgramName::Service)?;
            let stable_relay = stable_program_path(&root, ProgramName::McpRelay)?;
            assert_eq!(
                active_release_root_for_installed_program(&stable_cli, ProgramName::Cli)?,
                Some(active_release_root(&root)?)
            );
            assert_eq!(
                installation_root_for_installed_program(&stable_cli, ProgramName::Cli)?,
                Some(root.clone())
            );
            assert_eq!(
                installation_root_for_installed_program(
                    &temporary.path().join("source/bin/market-squawk"),
                    ProgramName::Cli,
                )?,
                None
            );
            let altered_release = root.join("versions/altered/bin");
            fs::create_dir_all(&altered_release)?;
            let altered_cli = altered_release.join("market-squawk");
            fs::copy(&stable_cli, &altered_cli)?;
            assert!(matches!(
                installation_root_for_installed_program(&altered_cli, ProgramName::Cli),
                Err(InstallError::CorruptInstallation)
            ));
            fs::remove_dir_all(root.join("versions/altered"))?;
            let stable_capture = stable_program_path(&root, ProgramName::CaptureHelper)?;
            let stable_worker = stable_program_path(&root, ProgramName::OnnxWorker)?;
            assert_eq!(stable_service.parent(), stable_cli.parent());
            assert_eq!(stable_relay.parent(), stable_cli.parent());
            assert_eq!(stable_capture.parent(), stable_cli.parent());
            assert_eq!(stable_worker.parent(), stable_cli.parent());
            assert_eq!(fs::read(&stable_cli)?, b"0.1.0:bin/market-squawk");
            assert_eq!(
                fs::read(stable_service)?,
                b"0.1.0:bin/market-squawk-service"
            );
            assert_eq!(
                fs::read(stable_relay)?,
                b"0.1.0:bin/market-squawk-mcp-relay"
            );
            assert_eq!(
                fs::read(stable_capture)?,
                b"0.1.0:bin/market-squawk-capture-helper"
            );
            assert_eq!(
                fs::read(stable_worker)?,
                b"0.1.0:bin/market-squawk-onnx-worker"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let stable_cli = stable_program_path(&root, ProgramName::Cli)?;
            fs::set_permissions(&stable_cli, fs::Permissions::from_mode(0o400))?;
            assert!(!status(&root)?.is_healthy());
            assert!(repair(RepairRequest::new(root.clone()))?.repaired());
            let repaired_cli = stable_program_path(&root, ProgramName::Cli)?;
            assert_ne!(fs::metadata(&repaired_cli)?.permissions().mode() & 0o111, 0);
        }

        let installed_version = fs::read_dir(root.join("versions"))?
            .next()
            .transpose()?
            .ok_or_else(|| std::io::Error::other("installed version is unavailable"))?
            .path();
        let damaged =
            installed_version.join("lib/python3.14/site-packages/market_squawk/__init__.py");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            use std::os::unix::fs::symlink;

            let preserved = temporary.path().join("preserved-outside-version");
            fs::write(&preserved, b"preserve")?;
            let parent = damaged
                .parent()
                .ok_or_else(|| std::io::Error::other("damaged component has no parent"))?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
            fs::remove_file(&damaged)?;
            symlink(&preserved, &damaged)?;
            let repaired = repair(RepairRequest::new(root.clone()))?;
            assert!(repaired.repaired());
            assert_eq!(fs::read(preserved)?, b"preserve");

            crate::store::remove_tree(&installed_version)?;
            let external_version = temporary.path().join("external-version");
            fs::create_dir(&external_version)?;
            fs::write(external_version.join("preserved"), b"preserve")?;
            let original_mode = fs::metadata(&external_version)?.permissions().mode();
            symlink(&external_version, &installed_version)?;

            let repaired = repair(RepairRequest::new(root.clone()))?;
            assert!(repaired.repaired());
            assert_eq!(fs::read(external_version.join("preserved"))?, b"preserve");
            assert_eq!(
                fs::metadata(&external_version)?.permissions().mode(),
                original_mode
            );
        }
        #[cfg(windows)]
        {
            let mut permissions = fs::metadata(&damaged)?.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&damaged, permissions)?;
            fs::write(&damaged, b"damaged")?;
            let repaired = repair(RepairRequest::new(root.clone()))?;
            assert!(repaired.repaired());
        }
        assert!(status(&root)?.is_healthy());

        let active = fs::read_dir(root.join("versions"))?
            .next()
            .transpose()?
            .ok_or_else(|| std::io::Error::other("active version is unavailable"))?
            .path();
        let damaged = active.join("lib/python3.14/site-packages/market_squawk/__init__.py");
        let release_cache = fs::read_dir(root.join("releases"))?
            .next()
            .transpose()?
            .ok_or_else(|| std::io::Error::other("release cache is unavailable"))?
            .path()
            .join("bundle.zip");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(&release_cache, fs::Permissions::from_mode(0o600))?;
        }
        #[cfg(windows)]
        {
            let mut permissions = fs::metadata(&release_cache)?.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&release_cache, permissions)?;
        }
        fs::write(&release_cache, b"damaged cache")?;
        let damaged_recovery = program_install_snapshot(&root, ProgramName::Cli)?;
        assert!(damaged_recovery.status().is_healthy());
        assert!(!damaged_recovery.recovery_ready());
        repair(RepairRequest::from_local(
            root.clone(),
            &first.manifest,
            &first.bundle,
        )?)?;
        assert_eq!(fs::read(&release_cache)?, fs::read(&first.bundle)?);
        assert!(program_install_snapshot(&root, ProgramName::Cli)?.recovery_ready());

        let retired_release_cache = root.join("staging/release-retired-fixture");
        fs::create_dir(&retired_release_cache)?;
        fs::write(retired_release_cache.join("retained"), b"remove")?;
        assert!(status(&root)?.is_healthy());
        assert!(!retired_release_cache.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(&damaged, fs::Permissions::from_mode(0o600))?;
            fs::set_permissions(&release_cache, fs::Permissions::from_mode(0o600))?;
        }
        #[cfg(windows)]
        for path in [&damaged, &release_cache] {
            let mut permissions = fs::metadata(path)?.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(path, permissions)?;
        }
        fs::write(&damaged, b"damaged")?;
        fs::write(&release_cache, b"damaged")?;

        let repaired = repair(
            RepairRequest::from_local(root.clone(), &first.manifest, &first.bundle)?
                .with_channel_manifest_url("https://example.com/release.json")?,
        )?;
        assert!(repaired.repaired());
        let recovered = status(&root)?;
        assert!(recovered.is_healthy());
        assert_eq!(
            recovered.channel_manifest_url(),
            Some("https://example.com/release.json")
        );

        let second = BundleFixture::create(temporary.path(), "0.2.0", BundleDefect::None)?;
        update(UpdateRequest::from_local(
            root.clone(),
            &second.manifest,
            &second.bundle,
        )?)?;
        #[cfg(unix)]
        assert_eq!(
            fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
            b"0.2.0:bin/market-squawk"
        );

        let updated = status(&root)?;
        assert_eq!(updated.active_version(), Some("0.2.0"));
        assert_eq!(updated.previous_version(), Some("0.1.0"));

        let previous_component = fs::read_dir(root.join("versions"))?
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("0.1.0-"))
            .ok_or_else(|| std::io::Error::other("previous version is unavailable"))?
            .path()
            .join("lib/python3.14/site-packages/market_squawk/__init__.py");
        let previous_cache = root
            .join("releases")
            .join(sha256_bytes(&first.manifest))
            .join("bundle.zip");
        for path in [&previous_component, &previous_cache] {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;

                fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
            }
            #[cfg(windows)]
            {
                let mut permissions = fs::metadata(path)?.permissions();
                permissions.set_readonly(false);
                fs::set_permissions(path, permissions)?;
            }
            fs::write(path, b"damaged")?;
        }
        rollback(
            RollbackRequest::from_local(root.clone(), &first.manifest, &first.bundle)?
                .with_channel_manifest_url("https://example.com/release.json")?,
        )?;
        let rolled_back = status(&root)?;
        assert_eq!(rolled_back.active_version(), Some("0.1.0"));
        assert_eq!(rolled_back.previous_version(), Some("0.2.0"));
        assert_eq!(
            rolled_back.channel_manifest_url(),
            Some("https://example.com/release.json")
        );
        assert!(rolled_back.is_healthy());
        #[cfg(unix)]
        assert_eq!(
            fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
            b"0.1.0:bin/market-squawk"
        );

        let active_component = fs::read_dir(root.join("versions"))?
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("0.1.0-"))
            .ok_or_else(|| std::io::Error::other("active version is unavailable"))?
            .path()
            .join("lib/python3.14/site-packages/market_squawk/__init__.py");
        let retained_component = fs::read_dir(root.join("versions"))?
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("0.2.0-"))
            .ok_or_else(|| std::io::Error::other("retained version is unavailable"))?
            .path()
            .join("lib/python3.14/site-packages/market_squawk/__init__.py");
        let active_cache = root
            .join("releases")
            .join(sha256_bytes(&first.manifest))
            .join("bundle.zip");
        let retained_cache = root
            .join("releases")
            .join(sha256_bytes(&second.manifest))
            .join("bundle.zip");
        for path in [
            &active_component,
            &retained_component,
            &active_cache,
            &retained_cache,
        ] {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;

                fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
            }
            #[cfg(windows)]
            {
                let mut permissions = fs::metadata(path)?.permissions();
                permissions.set_readonly(false);
                fs::set_permissions(path, permissions)?;
            }
            fs::write(path, b"damaged")?;
        }

        update(UpdateRequest::from_local(
            root.clone(),
            &second.manifest,
            &second.bundle,
        )?)?;
        let recovered_previous = status(&root)?;
        assert_eq!(recovered_previous.active_version(), Some("0.2.0"));
        assert_eq!(recovered_previous.previous_version(), None);
        assert!(recovered_previous.is_healthy());
        Ok(())
    }

    #[test]
    #[ignore = "run only from the isolated installed-package evidence lane"]
    fn signed_update_fixture_activates_and_rolls_back_an_installed_package() -> TestResult {
        let request_path = env::var_os("MARKET_SQUAWK_UPDATE_FIXTURE_REQUEST")
            .ok_or("MARKET_SQUAWK_UPDATE_FIXTURE_REQUEST is required")?;
        let request = SignedUpdateFixtureRequest::load(Path::new(&request_path))?;
        let receipt = run_signed_update_fixture(request)?;
        receipt.persist()?;
        Ok(())
    }

    #[test]
    fn failed_service_health_restores_the_known_good_release() -> TestResult {
        let temporary = test_directory()?;
        let root = temporary.path().join("program");
        let first = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &first.manifest,
            &first.bundle,
        )?)?;
        let candidate = BundleFixture::create(
            temporary.path(),
            "0.2.0",
            BundleDefect::ServiceHealthFailure,
        )?;

        let result = update(UpdateRequest::from_local(
            root.clone(),
            &candidate.manifest,
            &candidate.bundle,
        )?);

        assert!(result.is_err());
        let restored = status(&root)?;
        assert_eq!(restored.active_version(), Some("0.1.0"));
        assert!(restored.is_healthy());
        Ok(())
    }

    #[test]
    fn update_preserves_the_last_known_good_previous_version() -> TestResult {
        let temporary = test_directory()?;
        let root = temporary.path().join("program");
        let first = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &first.manifest,
            &first.bundle,
        )?)?;
        let second = BundleFixture::create(temporary.path(), "0.2.0", BundleDefect::None)?;
        update(UpdateRequest::from_local(
            root.clone(),
            &second.manifest,
            &second.bundle,
        )?)?;

        let active = fs::read_dir(root.join("versions"))?
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("0.2.0-"))
            .ok_or_else(|| std::io::Error::other("active version is unavailable"))?
            .path();
        let damaged = active.join("lib/python3.14/site-packages/market_squawk/__init__.py");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(&damaged, fs::Permissions::from_mode(0o600))?;
        }
        #[cfg(windows)]
        {
            let mut permissions = fs::metadata(&damaged)?.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&damaged, permissions)?;
        }
        fs::write(&damaged, b"damaged")?;

        let third = BundleFixture::create(temporary.path(), "0.3.0", BundleDefect::None)?;
        update(UpdateRequest::from_local(
            root.clone(),
            &third.manifest,
            &third.bundle,
        )?)?;

        let updated = status(&root)?;
        assert_eq!(updated.active_version(), Some("0.3.0"));
        assert_eq!(updated.previous_version(), Some("0.1.0"));
        rollback(RollbackRequest::new(root.clone()))?;
        assert_eq!(status(&root)?.active_version(), Some("0.1.0"));

        let active = fs::read_dir(root.join("versions"))?
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("0.1.0-"))
            .ok_or_else(|| std::io::Error::other("active version is unavailable"))?
            .path();
        let damaged = active.join("lib/python3.14/site-packages/market_squawk/__init__.py");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(&damaged, fs::Permissions::from_mode(0o600))?;
        }
        #[cfg(windows)]
        {
            let mut permissions = fs::metadata(&damaged)?.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&damaged, permissions)?;
        }
        fs::write(&damaged, b"damaged")?;

        update(UpdateRequest::from_local(
            root.clone(),
            &third.manifest,
            &third.bundle,
        )?)?;
        let reactivated = status(&root)?;
        assert_eq!(reactivated.active_version(), Some("0.3.0"));
        assert_eq!(reactivated.previous_version(), None);
        assert!(reactivated.is_healthy());
        Ok(())
    }

    #[test]
    fn pending_activation_recovers_the_selector_and_stable_program_set() -> TestResult {
        let temporary = test_directory()?;
        let root = temporary.path().join("program");
        let first = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &first.manifest,
            &first.bundle,
        )?)?;
        let second = BundleFixture::create(temporary.path(), "0.2.0", BundleDefect::None)?;
        update(UpdateRequest::from_local(
            root.clone(),
            &second.manifest,
            &second.bundle,
        )?)?;
        let pending = fs::read(root.join("installation.json"))?;
        rollback(RollbackRequest::new(root.clone()))?;
        assert_eq!(status(&root)?.active_version(), Some("0.1.0"));
        #[cfg(unix)]
        assert_eq!(
            fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
            b"0.1.0:bin/market-squawk"
        );

        fs::write(root.join("activation.json"), pending)?;
        #[cfg(windows)]
        crate::store::secure_test_store_file(&root.join("activation.json"))?;

        let pruning_obstruction = root.join("versions/pruning-obstruction");
        fs::write(&pruning_obstruction, b"obstruct pruning")?;
        #[cfg(windows)]
        crate::store::secure_test_store_file(&pruning_obstruction)?;
        assert!(matches!(
            status(&root),
            Err(InstallError::Store(StoreError::UnsafeRoot))
        ));
        assert!(root.join("activation.json").exists());
        fs::remove_file(pruning_obstruction)?;

        let recovered = status(&root)?;
        assert_eq!(recovered.active_version(), Some("0.2.0"));
        assert_eq!(recovered.previous_version(), Some("0.1.0"));
        assert!(recovered.is_healthy());
        assert!(!root.join("activation.json").exists());
        #[cfg(unix)]
        {
            assert_eq!(
                fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
                b"0.2.0:bin/market-squawk"
            );
            assert_eq!(
                fs::read(stable_program_path(&root, ProgramName::CaptureHelper)?)?,
                b"0.2.0:bin/market-squawk-capture-helper"
            );
            assert_eq!(
                fs::read(stable_program_path(&root, ProgramName::OnnxWorker)?)?,
                b"0.2.0:bin/market-squawk-onnx-worker"
            );
        }
        Ok(())
    }

    #[test]
    fn default_uninstall_preserves_separately_rooted_data() -> TestResult {
        let temporary = test_directory()?;
        let root = temporary.path().join("program");
        let data = temporary.path().join("data");
        fs::create_dir(&data)?;
        fs::write(data.join("portfolio.json"), b"preserve")?;
        let data = data.canonicalize()?;
        let fixture = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &fixture.manifest,
            &fixture.bundle,
        )?)?;

        let receipt = uninstall(UninstallRequest::preserving_data(root.clone()))?;

        assert!(receipt.removed_program());
        assert!(!root.exists());
        assert_eq!(fs::read(data.join("portfolio.json"))?, b"preserve");

        let detached = temporary.path().join(format!(
            ".market-squawk-program-removing-{}",
            uuid::Uuid::new_v4().as_simple()
        ));
        fs::create_dir(&detached)?;
        fs::write(detached.join("retained"), b"remove")?;

        let receipt = uninstall(
            UninstallRequest::preserving_data(root)
                .confirm_delete(MutableDataClass::Portfolios, data.clone()),
        )?;
        assert!(!receipt.removed_program());
        assert!(!detached.exists());
        assert!(!data.exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn uninstall_rejects_a_data_root_with_a_symlinked_ancestor() -> TestResult {
        use std::os::unix::fs::symlink;

        let temporary = test_directory()?;
        let base = temporary.path().canonicalize()?;
        let root = base.join("program");
        let actual_data = base.join("actual-data");
        let logs = actual_data.join("logs");
        fs::create_dir_all(&logs)?;
        fs::write(logs.join("market-squawk.log"), b"preserve")?;
        let alias = base.join("data-alias");
        symlink(&actual_data, &alias)?;
        let fixture = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &fixture.manifest,
            &fixture.bundle,
        )?)?;

        let result = uninstall(
            UninstallRequest::preserving_data(root.clone())
                .confirm_delete(MutableDataClass::Logs, alias.join("logs")),
        );

        assert!(matches!(result, Err(InstallError::UnsafeDataRoot)));
        assert!(root.exists());
        assert_eq!(fs::read(logs.join("market-squawk.log"))?, b"preserve");
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum BundleDefect {
        None,
        ParentTraversal,
        UnlistedEntry,
        DigestMismatch,
        MisplacedRequiredRole,
        ServiceHealthFailure,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct SignedUpdateFixtureRequest {
        install_root: PathBuf,
        manifest: PathBuf,
        bundle: PathBuf,
        evidence_directory: PathBuf,
    }

    impl SignedUpdateFixtureRequest {
        fn load(path: &Path) -> TestResult<Self> {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.file_type().is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() == 0
                || metadata.len() > 64 * 1024
            {
                return Err("fixture request must be one bounded regular file".into());
            }
            let request: Self = serde_json::from_slice(&fs::read(path)?)?;
            if !request.install_root.is_absolute()
                || !request.manifest.is_absolute()
                || !request.bundle.is_absolute()
                || !request.evidence_directory.is_absolute()
                || request.evidence_directory.exists()
            {
                return Err("fixture request paths are not isolated absolute paths".into());
            }
            Ok(request)
        }
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct SignedUpdateFixtureReceipt {
        schema_version: u16,
        evidence_kind: &'static str,
        production_update_availability: &'static str,
        target: &'static str,
        initial_version: String,
        activated_version: String,
        rolled_back_version: String,
        root_version: u64,
        timestamp_version: u64,
        snapshot_version: u64,
        targets_version: u64,
        pinned_root_sha256: String,
        rotated_root_sha256: String,
        timestamp_sha256: String,
        snapshot_sha256: String,
        targets_sha256: String,
        manifest_sha256: String,
        archive_sha256: String,
        #[serde(skip)]
        path: PathBuf,
    }

    impl SignedUpdateFixtureReceipt {
        fn persist(&self) -> TestResult {
            let mut encoded = serde_json::to_vec_pretty(self)?;
            encoded.push(b'\n');
            let mut output = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.path)?;
            output.write_all(&encoded)?;
            output.sync_all()?;
            Ok(())
        }
    }

    fn run_signed_update_fixture(
        request: SignedUpdateFixtureRequest,
    ) -> TestResult<SignedUpdateFixtureReceipt> {
        let initial = status(&request.install_root)?;
        let initial_version = initial
            .active_version()
            .ok_or("fixture install root has no active version")?
            .to_owned();
        if !initial.is_healthy() || initial.previous_version().is_some() {
            return Err("fixture requires one healthy initial installed generation".into());
        }
        let baseline_manifest = bounded_regular_file(&request.manifest, 8 * 1024 * 1024)?;
        let baseline_manifest_sha256 = sha256_bytes(&baseline_manifest);
        if initial.manifest_sha256.as_deref() != Some(baseline_manifest_sha256.as_str()) {
            return Err("fixture manifest does not identify the installed generation".into());
        }
        assert_production_updates_unavailable(&request.install_root)?;

        prepare_private_evidence_directory(&request.evidence_directory)?;
        let candidate_version = next_patch_version(&initial_version)?;
        let candidate_manifest = derive_candidate_manifest(&baseline_manifest, &candidate_version)?;
        let target = SupportedTarget::current()?;
        let fixture = crate::update_metadata::tests::SignedRepositoryFixture::for_release(
            candidate_manifest,
            &request.bundle,
            target.as_str(),
            &candidate_version,
        )?;
        let repository = request.evidence_directory.join("repository");
        write_signed_repository(&repository, &fixture, &request.bundle)?;

        let pending = fixture.pending(&request.install_root, &request.bundle)?;
        let activated = update(UpdateRequest::from_trusted_local(
            request.install_root.clone(),
            &fixture.manifest,
            &request.bundle,
            pending,
            &fixture.manifest_target_path,
            &fixture.archive_target_path,
        )?)?;
        let activated_status = status(&request.install_root)?;
        if activated.version() != candidate_version
            || activated_status.active_version() != Some(candidate_version.as_str())
            || activated_status.previous_version() != Some(initial_version.as_str())
            || !activated_status.is_healthy()
        {
            return Err("signed fixture did not activate the expected immutable generation".into());
        }

        let rolled_back = rollback(RollbackRequest::new(request.install_root.clone()))?;
        let final_status = status(&request.install_root)?;
        if rolled_back.version() != initial_version
            || final_status.active_version() != Some(initial_version.as_str())
            || final_status.previous_version() != Some(candidate_version.as_str())
            || !final_status.is_healthy()
        {
            return Err("signed fixture did not restore the retained known-good generation".into());
        }
        let trust = fixture
            .pending(&request.install_root, &request.bundle)?
            .persist()?;
        Ok(SignedUpdateFixtureReceipt {
            schema_version: 1,
            evidence_kind: "installed-package-signed-update-rollback",
            production_update_availability: "production-signing-material-unavailable",
            target: target.as_str(),
            initial_version: initial_version.clone(),
            activated_version: candidate_version,
            rolled_back_version: initial_version,
            root_version: trust.root_version(),
            timestamp_version: trust.timestamp_version(),
            snapshot_version: trust.snapshot_version(),
            targets_version: trust.targets_version(),
            pinned_root_sha256: sha256_bytes(&fixture.pinned_root),
            rotated_root_sha256: sha256_bytes(&fixture.rotated_root),
            timestamp_sha256: sha256_bytes(&fixture.timestamp),
            snapshot_sha256: sha256_bytes(&fixture.snapshot),
            targets_sha256: sha256_bytes(&fixture.targets),
            manifest_sha256: sha256_bytes(&fixture.manifest),
            archive_sha256: hex_sha256(fixture.archive_sha256),
            path: request.evidence_directory.join("receipt.json"),
        })
    }

    fn assert_production_updates_unavailable(install_root: &Path) -> TestResult {
        let channel_path =
            active_release_root(install_root)?.join("share/market-squawk/update/channel.json");
        let channel: Value =
            serde_json::from_slice(&bounded_regular_file(&channel_path, 64 * 1024)?)?;
        if channel
            != json!({
                "availability": "unavailable",
                "reason": "production-signing-material-unavailable",
                "schemaVersion": 1
            })
        {
            return Err("shipping package must retain the unavailable production channel".into());
        }
        Ok(())
    }

    fn next_patch_version(current: &str) -> TestResult<String> {
        let current = Version::parse(current)?;
        let patch = current.patch.checked_add(1).ok_or("version overflow")?;
        Ok(format!("{}.{}.{patch}", current.major, current.minor))
    }

    fn derive_candidate_manifest(baseline_manifest: &[u8], version: &str) -> TestResult<Vec<u8>> {
        let mut manifest: Value = serde_json::from_slice(baseline_manifest)?;
        let object = manifest
            .as_object_mut()
            .ok_or("release manifest is not an object")?;
        object.insert("version".to_owned(), Value::String(version.to_owned()));
        object.insert("tag".to_owned(), Value::String(format!("v{version}")));
        let targets = object
            .get_mut("targets")
            .and_then(Value::as_array_mut)
            .ok_or("release manifest has no target array")?;
        let selected = SupportedTarget::current()?.as_str();
        let mut selected_found = false;
        for target in targets {
            let target_object = target.as_object_mut().ok_or("target is not an object")?;
            let target_name = target_object
                .get("target")
                .and_then(Value::as_str)
                .ok_or("target identity is absent")?
                .to_owned();
            let archive_object = target_object
                .get_mut("archive")
                .and_then(Value::as_object_mut)
                .ok_or("target archive identity is absent")?;
            archive_object.insert(
                "url".to_owned(),
                Value::String(format!(
                    "https://github.com/Sawmonabo/market-squawk/releases/download/v{version}/market-squawk-update-fixture-{target_name}.zip"
                )),
            );
            selected_found |= target_name == selected;
        }
        if !selected_found {
            return Err("release manifest does not contain the current target".into());
        }
        let mut encoded = serde_json::to_vec_pretty(&manifest)?;
        encoded.push(b'\n');
        crate::ReleaseManifest::admit_current(&encoded)?;
        Ok(encoded)
    }

    fn prepare_private_evidence_directory(path: &Path) -> TestResult {
        let parent = path.parent().ok_or("evidence directory has no parent")?;
        let parent_metadata = fs::symlink_metadata(parent)?;
        if !parent_metadata.is_dir() || parent_metadata.file_type().is_symlink() {
            return Err("evidence parent is not a regular directory".into());
        }
        fs::create_dir(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    fn write_signed_repository(
        root: &Path,
        fixture: &crate::update_metadata::tests::SignedRepositoryFixture,
        candidate_bundle: &Path,
    ) -> TestResult {
        fs::create_dir(root)?;
        for (relative, bytes) in [
            ("1.root.json", fixture.pinned_root.as_slice()),
            ("2.root.json", fixture.rotated_root.as_slice()),
            ("timestamp.json", fixture.timestamp.as_slice()),
            ("4.snapshot.json", fixture.snapshot.as_slice()),
            ("5.targets.json", fixture.targets.as_slice()),
            (&fixture.manifest_download_path, fixture.manifest.as_slice()),
        ] {
            write_repository_file(root, relative, bytes)?;
        }
        let archive = root.join(&fixture.archive_download_path);
        let parent = archive.parent().ok_or("archive target has no parent")?;
        fs::create_dir_all(parent)?;
        fs::copy(candidate_bundle, &archive)?;
        if sha256_file(&archive)? != hex_sha256(fixture.archive_sha256) {
            return Err("written archive target changed identity".into());
        }
        Ok(())
    }

    fn write_repository_file(root: &Path, relative: &str, bytes: &[u8]) -> TestResult {
        let path = root.join(relative);
        let parent = path.parent().ok_or("repository target has no parent")?;
        fs::create_dir_all(parent)?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        output.write_all(bytes)?;
        output.sync_all()?;
        Ok(())
    }

    fn bounded_regular_file(path: &Path, maximum: u64) -> TestResult<Vec<u8>> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() == 0
            || metadata.len() > maximum
        {
            return Err("fixture input is not a bounded regular file".into());
        }
        Ok(fs::read(path)?)
    }

    fn hex_sha256(bytes: [u8; 32]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    struct BundleFixture {
        bundle: PathBuf,
        manifest: Vec<u8>,
    }

    impl BundleFixture {
        fn create(root: &Path, version: &str, defect: BundleDefect) -> TestResult<Self> {
            crate::store::secure_test_store_parent(root)?;
            let bundle = root.join(format!("market-squawk-{version}.zip"));
            let target = SupportedTarget::current()?;
            let mut components = Vec::with_capacity(ComponentRole::REQUIRED.len());
            let file = File::create(&bundle)?;
            let mut archive = ZipWriter::new(file);
            for role in ComponentRole::REQUIRED {
                let executable = role.requires_executable();
                let expected_path = role.fixed_path(target).unwrap_or_else(|| {
                    "lib/python3.14/site-packages/market_squawk/__init__.py".to_owned()
                });
                let path = if matches!(defect, BundleDefect::MisplacedRequiredRole)
                    && role == ComponentRole::PythonRuntime
                {
                    "tools/python".to_owned()
                } else {
                    expected_path
                };
                let bytes = if matches!(defect, BundleDefect::ServiceHealthFailure)
                    && role == ComponentRole::Service
                {
                    b"market-squawk-test-service-health-failure".to_vec()
                } else {
                    format!("{version}:{path}").into_bytes()
                };
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Deflated)
                    .unix_permissions(if executable { 0o755 } else { 0o644 });
                archive.start_file(&path, options)?;
                archive.write_all(&bytes)?;
                let digest = if matches!(defect, BundleDefect::DigestMismatch)
                    && role == ComponentRole::Cli
                {
                    "0".repeat(64)
                } else {
                    sha256_bytes(&bytes)
                };
                components.push(json!({
                    "path": path,
                    "role": role,
                    "size": bytes.len(),
                    "sha256": digest,
                    "executable": executable,
                }));
            }
            if matches!(defect, BundleDefect::ParentTraversal) {
                archive.start_file(
                    "../escape",
                    SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
                )?;
                archive.write_all(b"escape")?;
            }
            if matches!(defect, BundleDefect::UnlistedEntry) {
                archive.start_file(
                    "unlisted.txt",
                    SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
                )?;
                archive.write_all(b"unlisted")?;
            }
            archive.finish()?;

            components.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
            let archive_size = fs::metadata(&bundle)?.len();
            let archive_sha256 = sha256_file(&bundle)?;
            let manifest = serde_json::to_vec_pretty(&json!({
                "schema_version": 2,
                "product": "market-squawk",
                "version": version,
                "tag": format!("v{version}"),
                "repository": "Sawmonabo/market-squawk",
                "commit_sha": "a".repeat(40),
                "tree_sha": "b".repeat(40),
                "generated_at": "2026-07-29T00:00:00Z",
                "targets": [{
                    "target": target.as_str(),
                    "minimum_system": target.minimum_system(),
                    "native_trust_mode": "provenance-only",
                    "archive": {
                    "url": format!(
                        "https://github.com/Sawmonabo/market-squawk/releases/download/v{version}/market-squawk-{version}.zip"
                    ),
                        "size": archive_size,
                        "sha256": archive_sha256,
                    },
                    "components": components,
                }],
            }))?;

            Ok(Self { bundle, manifest })
        }
    }

    fn sha256_bytes(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn sha256_file(path: &Path) -> TestResult<String> {
        let mut file = File::open(path)?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 8 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(format!("{:x}", digest.finalize()))
    }
}
