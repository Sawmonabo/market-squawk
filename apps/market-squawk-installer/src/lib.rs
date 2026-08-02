//! Verified, versioned Market Squawk installation lifecycle.

mod archive;
mod command;
mod contracts;
mod lifecycle;
mod manifest;
mod platform;
mod store;

pub use self::archive::ArchiveError;
pub use self::command::{CommandError, run_cli, update_from_channel};
pub use self::contracts::{
    InstallReceipt, InstallRequest, InstallStatus, MutableDataClass, ProgramInstallSnapshot,
    RepairRequest, RollbackRequest, UninstallReceipt, UninstallRequest, UpdateRequest,
};
pub use self::lifecycle::{
    InstallError, active_program_path, active_release_root,
    active_release_root_for_installed_program, install, program_install_snapshot, repair, rollback,
    stable_program_path, status, uninstall, update,
};
pub use self::manifest::{
    AdmittedRelease, ComponentRole, MAXIMUM_MANIFEST_BYTES, ManifestError, ReleaseManifest,
};
pub use self::platform::{
    NativeTrustMode, PlatformError, ProgramName, SupportedTarget, default_install_root,
};
pub use self::store::StoreError;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs::{self, File};
    use std::io::{Read as _, Write as _};
    use std::path::{Path, PathBuf};

    use serde_json::json;
    use sha2::{Digest as _, Sha256};
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    use super::{
        ComponentRole, InstallError, InstallRequest, MutableDataClass, ProgramName, RepairRequest,
        RollbackRequest, StoreError, SupportedTarget, UninstallRequest, UpdateRequest, install,
        program_install_snapshot, repair, rollback, status, uninstall, update,
    };
    #[cfg(unix)]
    use super::{
        active_release_root, active_release_root_for_installed_program, stable_program_path,
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
        Ok(tempfile::Builder::new()
            .prefix("market-squawk-installer-")
            .tempdir_in(local_app_data)?)
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
            assert_eq!(
                active_release_root_for_installed_program(&stable_cli, ProgramName::Cli)?,
                Some(active_release_root(&root)?)
            );
            let stable_capture = stable_program_path(&root, ProgramName::CaptureHelper)?;
            let stable_worker = stable_program_path(&root, ProgramName::OnnxWorker)?;
            assert_eq!(stable_service.parent(), stable_cli.parent());
            assert_eq!(stable_capture.parent(), stable_cli.parent());
            assert_eq!(stable_worker.parent(), stable_cli.parent());
            assert_eq!(fs::read(&stable_cli)?, b"0.1.0:bin/market-squawk");
            assert_eq!(
                fs::read(stable_service)?,
                b"0.1.0:bin/market-squawk-service"
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
                let bytes = format!("{version}:{path}").into_bytes();
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
