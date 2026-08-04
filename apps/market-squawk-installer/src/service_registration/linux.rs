//! Linux per-user systemd service registration without linger.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use directories::BaseDirs;

use super::{
    NativeRegistrationSnapshot, PreparedRegistration, ServiceRegistrationError, native_document,
    run_bounded, sha256_bytes,
};

pub(super) const REGISTRATION_IDENTITY: &str = "market-squawk.service";
const SYSTEMCTL: &str = "/usr/bin/systemctl";
const OWNER_LINE: &str = "# owner:market-squawk-installer-v1";

pub(super) fn render_user_unit(
    service: &Path,
    workspace_data_root: &Path,
    installation_data_root: &Path,
    release_root: &Path,
) -> Result<String, ServiceRegistrationError> {
    let service = systemd_quote(service)?;
    let workspace_data_root = systemd_quote(workspace_data_root)?;
    let installation_data_root = systemd_quote(installation_data_root)?;
    let release_root = systemd_quote(release_root)?;
    let invocation = format!(
        "{service} --data-dir {workspace_data_root} \
         --installation-data-root {installation_data_root} \
         --training-release-root {release_root}"
    );
    Ok(format!(
        "{OWNER_LINE}\n\
         [Unit]\n\
         Description=Market Squawk installed application service\n\
         Documentation=https://github.com/Sawmonabo/market-squawk\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         WorkingDirectory={installation_data_root}\n\
         ExecStart={invocation}\n\
         Restart=on-failure\n\
         RestartSec=5s\n\
         TimeoutStopSec=15s\n\
         KillMode=control-group\n\
         UMask=0077\n\
         NoNewPrivileges=yes\n\
         PrivateTmp=yes\n\
         PrivateDevices=yes\n\
         ProtectSystem=full\n\
         ProtectControlGroups=yes\n\
         ProtectKernelTunables=yes\n\
         ProtectKernelModules=yes\n\
         RestrictSUIDSGID=yes\n\
         LockPersonality=yes\n\
         RestrictRealtime=yes\n\
         SystemCallArchitectures=native\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    ))
}

pub(super) fn prepare(
    service: &Path,
    workspace_data_root: &Path,
    installation_data_root: &Path,
    release_root: &Path,
) -> Result<PreparedRegistration, ServiceRegistrationError> {
    let document = native_document(
        render_user_unit(
            service,
            workspace_data_root,
            installation_data_root,
            release_root,
        )?
        .into_bytes(),
    )?;
    Ok(PreparedRegistration {
        identity: REGISTRATION_IDENTITY,
        configuration_sha256: sha256_bytes(&document),
        document,
    })
}

pub(super) fn inspect() -> Result<Option<NativeRegistrationSnapshot>, ServiceRegistrationError> {
    let path = unit_path()?;
    let Some(document) = read_registration_file(&path)? else {
        return Ok(None);
    };
    let text =
        std::str::from_utf8(&document).map_err(|_| ServiceRegistrationError::NativeDocument)?;
    Ok(Some(NativeRegistrationSnapshot {
        configuration_sha256: sha256_bytes(&document),
        owned: text.lines().next() == Some(OWNER_LINE),
        document,
    }))
}

pub(super) fn apply(prepared: &PreparedRegistration) -> Result<(), ServiceRegistrationError> {
    if prepared.identity != REGISTRATION_IDENTITY {
        return Err(ServiceRegistrationError::Identity);
    }
    write_registration_file(&unit_path()?, &prepared.document)?;
    daemon_reload()?;
    run_systemctl(["enable", REGISTRATION_IDENTITY])?;
    Ok(())
}

pub(super) fn start() -> Result<(), ServiceRegistrationError> {
    run_systemctl(["restart", REGISTRATION_IDENTITY])?;
    Ok(())
}

pub(super) fn restart() -> Result<(), ServiceRegistrationError> {
    start()
}

pub(super) fn ensure_active() -> Result<(), ServiceRegistrationError> {
    run_systemctl(["is-active", "--quiet", REGISTRATION_IDENTITY])?;
    Ok(())
}

pub(super) fn remove(
    expected: &NativeRegistrationSnapshot,
) -> Result<(), ServiceRegistrationError> {
    let current = inspect()?.ok_or(ServiceRegistrationError::RegistrationMissing)?;
    if !current.owned
        || current.configuration_sha256.as_ref() != expected.configuration_sha256.as_ref()
    {
        return Err(ServiceRegistrationError::Conflict);
    }
    disable_if_present()?;
    let path = unit_path()?;
    fs::remove_file(&path)
        .map_err(|source| ServiceRegistrationError::io("remove systemd user unit", source))?;
    sync_parent(&path)?;
    daemon_reload()
}

pub(super) fn restore(
    prior: Option<&NativeRegistrationSnapshot>,
    attempted: &PreparedRegistration,
) -> Result<(), ServiceRegistrationError> {
    if let Some(current) = inspect()? {
        if !current.owned
            || current.configuration_sha256.as_ref() != attempted.configuration_sha256.as_ref()
        {
            return Err(ServiceRegistrationError::Conflict);
        }
        disable_if_present()?;
    }
    let path = unit_path()?;
    match prior {
        Some(prior) if prior.owned => {
            write_registration_file(&path, &prior.document)?;
            daemon_reload()?;
            run_systemctl(["enable", REGISTRATION_IDENTITY])?;
            start()
        }
        Some(_) => Err(ServiceRegistrationError::Conflict),
        None => {
            match fs::remove_file(&path) {
                Ok(()) => sync_parent(&path)?,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(ServiceRegistrationError::io(
                        "remove failed systemd user unit",
                        source,
                    ));
                }
            }
            daemon_reload()
        }
    }
}

fn run_systemctl<const N: usize>(arguments: [&str; N]) -> Result<(), ServiceRegistrationError> {
    let mut args = Vec::with_capacity(N + 1);
    args.push(OsString::from("--user"));
    args.extend(arguments.into_iter().map(OsString::from));
    run_bounded(Path::new(SYSTEMCTL), args, false)?;
    Ok(())
}

fn daemon_reload() -> Result<(), ServiceRegistrationError> {
    run_systemctl(["daemon-reload"])
}

fn disable_if_present() -> Result<(), ServiceRegistrationError> {
    run_systemctl(["stop", REGISTRATION_IDENTITY])?;
    run_systemctl(["disable", REGISTRATION_IDENTITY])
}

fn unit_path() -> Result<PathBuf, ServiceRegistrationError> {
    let base = BaseDirs::new().ok_or(ServiceRegistrationError::UnsafePath)?;
    Ok(base
        .config_dir()
        .join("systemd/user")
        .join(REGISTRATION_IDENTITY))
}

fn systemd_quote(path: &Path) -> Result<String, ServiceRegistrationError> {
    let value = path.to_str().ok_or(ServiceRegistrationError::Identity)?;
    if value.chars().any(|character| character.is_control()) {
        return Err(ServiceRegistrationError::Identity);
    }
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
        .replace('$', "$$");
    Ok(format!("\"{escaped}\""))
}

fn read_registration_file(path: &Path) -> Result<Option<Vec<u8>>, ServiceRegistrationError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ServiceRegistrationError::io(
                "inspect systemd user unit",
                source,
            ));
        }
    };
    validate_owned_file(&metadata)?;
    let bytes = fs::read(path)
        .map_err(|source| ServiceRegistrationError::io("read systemd user unit", source))?;
    native_document(bytes).map(Some)
}

fn write_registration_file(path: &Path, bytes: &[u8]) -> Result<(), ServiceRegistrationError> {
    native_document(bytes.to_vec())?;
    let parent = path.parent().ok_or(ServiceRegistrationError::UnsafePath)?;
    fs::create_dir_all(parent)
        .map_err(|source| ServiceRegistrationError::io("create systemd user directory", source))?;
    validate_owned_directory(parent)?;
    if let Ok(metadata) = fs::symlink_metadata(path) {
        validate_owned_file(&metadata)?;
    }
    let atomic = AtomicFile::new(path, AllowOverwrite);
    atomic
        .write(|file| {
            use std::os::unix::fs::PermissionsExt as _;

            file.write_all(bytes)?;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
            file.sync_all()
        })
        .map_err(|error| {
            let source: std::io::Error = error.into();
            ServiceRegistrationError::io("publish systemd user unit", source)
        })?;
    sync_parent(path)
}

fn validate_owned_directory(path: &Path) -> Result<(), ServiceRegistrationError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = fs::symlink_metadata(path)
        .map_err(|source| ServiceRegistrationError::io("inspect systemd user directory", source))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    Ok(())
}

fn validate_owned_file(metadata: &fs::Metadata) -> Result<(), ServiceRegistrationError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(ServiceRegistrationError::Conflict);
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), ServiceRegistrationError> {
    let parent = path.parent().ok_or(ServiceRegistrationError::UnsafePath)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            ServiceRegistrationError::io("synchronize systemd user directory", source)
        })
}
