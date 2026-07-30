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
    InstallReceipt, InstallRequest, InstallStatus, MutableDataClass, RepairRequest,
    RollbackRequest, UninstallReceipt, UninstallRequest, UpdateRequest,
};
pub use self::lifecycle::{
    InstallError, active_release_root, install, repair, rollback, stable_program_path, status,
    uninstall, update,
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
        InstallRequest, RepairRequest, RollbackRequest, SupportedTarget, UninstallRequest,
        UpdateRequest, install, repair, rollback, status, uninstall, update,
    };
    #[cfg(unix)]
    use super::{ProgramName, stable_program_path};

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    const COMPONENTS: [(&str, &str, bool); 10] = [
        ("bin/market-squawk-desktop", "desktop", true),
        ("bin/market-squawk", "cli", true),
        ("bin/market-squawk-capture-helper", "capture-helper", true),
        ("bin/market-squawk-onnx-worker", "onnx-worker", true),
        ("bin/market-squawk-model-validator", "model-validator", true),
        ("bin/market-squawk-train", "training-driver", true),
        ("bin/market-squawk-installer", "installer", true),
        ("tools/uv", "uv", true),
        ("bin/python", "python-runtime", true),
        (
            "lib/python3.14/site-packages/market_squawk/__init__.py",
            "python-environment",
            false,
        ),
    ];

    #[test]
    fn rejects_unsafe_unlisted_or_mismatched_bundle_before_activation() -> TestResult {
        for defect in [
            BundleDefect::ParentTraversal,
            BundleDefect::UnlistedEntry,
            BundleDefect::DigestMismatch,
        ] {
            let temporary = TempDir::new()?;
            let fixture = BundleFixture::create(temporary.path(), "0.1.0", defect)?;
            let result = install(InstallRequest::from_local(
                temporary.path().join("program"),
                &fixture.manifest,
                &fixture.bundle,
            )?);

            assert!(result.is_err());
            assert!(!temporary.path().join("program/installation.json").exists());
        }
        Ok(())
    }

    #[test]
    fn activation_and_rollback_switch_complete_versions() -> TestResult {
        let temporary = TempDir::new()?;
        let root = temporary.path().join("program");
        let first = BundleFixture::create(temporary.path(), "0.1.0", BundleDefect::None)?;
        install(InstallRequest::from_local(
            root.clone(),
            &first.manifest,
            &first.bundle,
        )?)?;
        #[cfg(unix)]
        assert_eq!(
            fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
            b"0.1.0:bin/market-squawk"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let stable_cli = stable_program_path(&root, ProgramName::Cli)?;
            fs::set_permissions(&stable_cli, fs::Permissions::from_mode(0o700))?;
            fs::write(&stable_cli, b"damaged")?;
            assert!(!status(&root)?.is_healthy());
            assert!(repair(RepairRequest::new(root.clone()))?.repaired());
            assert_eq!(
                fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
                b"0.1.0:bin/market-squawk"
            );
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

            fs::set_permissions(&damaged, fs::Permissions::from_mode(0o600))?;
        }
        #[cfg(windows)]
        {
            let mut permissions = fs::metadata(&damaged)?.permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&damaged, permissions)?;
        }
        fs::write(&damaged, b"damaged")?;
        let repaired = repair(RepairRequest::new(root.clone()))?;
        assert!(repaired.repaired());
        assert!(status(&root)?.is_healthy());

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

        rollback(RollbackRequest::new(root.clone()))?;
        let rolled_back = status(&root)?;
        assert_eq!(rolled_back.active_version(), Some("0.1.0"));
        assert_eq!(rolled_back.previous_version(), Some("0.2.0"));
        #[cfg(unix)]
        assert_eq!(
            fs::read(stable_program_path(&root, ProgramName::Cli)?)?,
            b"0.1.0:bin/market-squawk"
        );
        Ok(())
    }

    #[test]
    fn default_uninstall_preserves_separately_rooted_data() -> TestResult {
        let temporary = TempDir::new()?;
        let root = temporary.path().join("program");
        let data = temporary.path().join("data");
        fs::create_dir(&data)?;
        fs::write(data.join("portfolio.json"), b"preserve")?;
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
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum BundleDefect {
        None,
        ParentTraversal,
        UnlistedEntry,
        DigestMismatch,
    }

    struct BundleFixture {
        bundle: PathBuf,
        manifest: Vec<u8>,
    }

    impl BundleFixture {
        fn create(root: &Path, version: &str, defect: BundleDefect) -> TestResult<Self> {
            let bundle = root.join(format!("market-squawk-{version}.zip"));
            let mut components = Vec::with_capacity(COMPONENTS.len());
            let file = File::create(&bundle)?;
            let mut archive = ZipWriter::new(file);
            for (path, role, executable) in COMPONENTS {
                let bytes = format!("{version}:{path}").into_bytes();
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Deflated)
                    .unix_permissions(if executable { 0o755 } else { 0o644 });
                archive.start_file(path, options)?;
                archive.write_all(&bytes)?;
                let digest = if matches!(defect, BundleDefect::DigestMismatch) && role == "cli" {
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
            let target = SupportedTarget::current()?;
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
