//! Linux per-user systemd service registration without linger.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use directories::BaseDirs;

use super::{
    LinuxManagerState, NativeManagerState, NativeRegistrationSnapshot, PlatformServiceOperation,
    PreparedRegistration, ServiceRegistrationError, native_document, run_bounded, sha256_bytes,
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
         CapabilityBoundingSet=\n\
         AmbientCapabilities=\n\
         RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\n\
         RestrictNamespaces=yes\n\
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
    verify_loaded_unit()?;
    Ok(())
}

pub(super) fn start(replacing: bool) -> Result<(), ServiceRegistrationError> {
    if replacing {
        run_systemctl(
            PlatformServiceOperation::EnableRegistration,
            ["enable", REGISTRATION_IDENTITY],
        )?;
        run_systemctl(
            PlatformServiceOperation::RestartRegistration,
            ["restart", REGISTRATION_IDENTITY],
        )?;
    } else {
        run_systemctl(
            PlatformServiceOperation::StartRegistration,
            ["enable", "--now", REGISTRATION_IDENTITY],
        )?;
    }
    Ok(())
}

pub(super) fn restart() -> Result<(), ServiceRegistrationError> {
    run_systemctl(
        PlatformServiceOperation::RestartRegistration,
        ["restart", REGISTRATION_IDENTITY],
    )
}

pub(super) fn ensure_active() -> Result<(), ServiceRegistrationError> {
    verify_loaded_unit()?;
    run_systemctl(
        PlatformServiceOperation::InspectRegistration,
        ["is-active", "--quiet", REGISTRATION_IDENTITY],
    )?;
    Ok(())
}

pub(super) fn manager_state() -> Result<NativeManagerState, ServiceRegistrationError> {
    let registration = inspect()?;
    let manager = systemd_state()?;
    match (registration, manager.load_state.as_ref()) {
        (None, "not-found") if manager.fragment_path.is_empty() => Ok(NativeManagerState::Absent),
        (Some(_), "loaded") => {
            manager.verify_fragment()?;
            Ok(NativeManagerState::Linux(manager.observation()))
        }
        _ => Err(ServiceRegistrationError::Conflict),
    }
}

pub(super) fn prove_absent() -> Result<(), ServiceRegistrationError> {
    if inspect()?.is_some() {
        return Err(ServiceRegistrationError::Conflict);
    }
    let manager = systemd_state()?;
    if manager.load_state.as_ref() == "not-found" && manager.fragment_path.is_empty() {
        Ok(())
    } else {
        Err(ServiceRegistrationError::Conflict)
    }
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
    daemon_reload()?;
    prove_absent()
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
            verify_loaded_unit()?;
            run_systemctl(
                PlatformServiceOperation::EnableRegistration,
                ["enable", REGISTRATION_IDENTITY],
            )?;
            restart()
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
            daemon_reload()?;
            prove_absent()
        }
    }
}

fn run_systemctl<const N: usize>(
    operation: PlatformServiceOperation,
    arguments: [&str; N],
) -> Result<(), ServiceRegistrationError> {
    let mut args = Vec::with_capacity(N + 1);
    args.push(OsString::from("--user"));
    args.extend(arguments.into_iter().map(OsString::from));
    run_bounded(operation, Path::new(SYSTEMCTL), args, false)?;
    Ok(())
}

fn daemon_reload() -> Result<(), ServiceRegistrationError> {
    run_systemctl(PlatformServiceOperation::ReloadManager, ["daemon-reload"])
}

fn disable_if_present() -> Result<(), ServiceRegistrationError> {
    run_systemctl_cleanup(
        PlatformServiceOperation::StopRegistration,
        ["stop", REGISTRATION_IDENTITY],
    )?;
    run_systemctl_cleanup(
        PlatformServiceOperation::DisableRegistration,
        ["disable", REGISTRATION_IDENTITY],
    )
}

fn run_systemctl_cleanup<const N: usize>(
    operation: PlatformServiceOperation,
    arguments: [&str; N],
) -> Result<(), ServiceRegistrationError> {
    match run_systemctl(operation, arguments) {
        Ok(()) => Ok(()),
        Err(ServiceRegistrationError::CommandFailed(failure)) if failure.status == Some(5) => {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn verify_loaded_unit() -> Result<(), ServiceRegistrationError> {
    let manager = systemd_state()?;
    if manager.load_state.as_ref() != "loaded" {
        return Err(ServiceRegistrationError::RegistrationMissing);
    }
    manager.verify_fragment()
}

#[derive(Debug)]
struct SystemdState {
    load_state: Box<str>,
    fragment_path: Box<str>,
    active_state: Box<str>,
    sub_state: Box<str>,
    result: Box<str>,
    exec_main_code: i32,
    exec_main_status: i32,
    restarts: u64,
}

impl SystemdState {
    fn verify_fragment(&self) -> Result<(), ServiceRegistrationError> {
        let expected = fs::canonicalize(unit_path()?)
            .map_err(|source| ServiceRegistrationError::io("resolve systemd user unit", source))?;
        let observed =
            fs::canonicalize(PathBuf::from(self.fragment_path.as_ref())).map_err(|source| {
                ServiceRegistrationError::io("resolve loaded systemd user unit", source)
            })?;
        if observed == expected {
            Ok(())
        } else {
            Err(ServiceRegistrationError::Conflict)
        }
    }

    fn observation(&self) -> LinuxManagerState {
        LinuxManagerState {
            active_state: self.active_state.clone(),
            sub_state: self.sub_state.clone(),
            result: self.result.clone(),
            exec_main_code: self.exec_main_code,
            exec_main_status: self.exec_main_status,
            restarts: self.restarts,
        }
    }
}

fn systemd_state() -> Result<SystemdState, ServiceRegistrationError> {
    let mut arguments = Vec::with_capacity(13);
    arguments.push(OsString::from("--user"));
    arguments.extend([
        OsString::from("show"),
        OsString::from(REGISTRATION_IDENTITY),
        OsString::from("--property=LoadState"),
        OsString::from("--property=FragmentPath"),
        OsString::from("--property=ActiveState"),
        OsString::from("--property=SubState"),
        OsString::from("--property=Result"),
        OsString::from("--property=ExecMainCode"),
        OsString::from("--property=ExecMainStatus"),
        OsString::from("--property=NRestarts"),
        OsString::from("--no-pager"),
    ]);
    let output = run_bounded(
        PlatformServiceOperation::InspectRegistration,
        Path::new(SYSTEMCTL),
        arguments,
        true,
    )?;
    let text = std::str::from_utf8(&output.stdout).map_err(|_| {
        ServiceRegistrationError::CommandOutput(PlatformServiceOperation::InspectRegistration)
    })?;
    let mut load_state = None;
    let mut fragment_path = None;
    let mut active_state = None;
    let mut sub_state = None;
    let mut result = None;
    let mut exec_main_code = None;
    let mut exec_main_status = None;
    let mut restarts = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("LoadState=") {
            set_once(&mut load_state, bounded_systemd_token(value)?)?;
        } else if let Some(value) = line.strip_prefix("FragmentPath=") {
            if value.contains('\0') || value.len() > 4096 {
                return Err(ServiceRegistrationError::NativeDocument);
            }
            set_once(&mut fragment_path, value.into())?;
        } else if let Some(value) = line.strip_prefix("ActiveState=") {
            set_once(&mut active_state, bounded_systemd_token(value)?)?;
        } else if let Some(value) = line.strip_prefix("SubState=") {
            set_once(&mut sub_state, bounded_systemd_token(value)?)?;
        } else if let Some(value) = line.strip_prefix("Result=") {
            set_once(&mut result, bounded_systemd_token(value)?)?;
        } else if let Some(value) = line.strip_prefix("ExecMainCode=") {
            set_once(
                &mut exec_main_code,
                value
                    .parse::<i32>()
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?,
            )?;
        } else if let Some(value) = line.strip_prefix("ExecMainStatus=") {
            set_once(
                &mut exec_main_status,
                value
                    .parse::<i32>()
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?,
            )?;
        } else if let Some(value) = line.strip_prefix("NRestarts=") {
            set_once(
                &mut restarts,
                value
                    .parse::<u64>()
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?,
            )?;
        } else if !line.is_empty() {
            return Err(ServiceRegistrationError::NativeDocument);
        }
    }
    Ok(SystemdState {
        load_state: load_state.ok_or(ServiceRegistrationError::NativeDocument)?,
        fragment_path: fragment_path.ok_or(ServiceRegistrationError::NativeDocument)?,
        active_state: active_state.ok_or(ServiceRegistrationError::NativeDocument)?,
        sub_state: sub_state.ok_or(ServiceRegistrationError::NativeDocument)?,
        result: result.ok_or(ServiceRegistrationError::NativeDocument)?,
        exec_main_code: exec_main_code.ok_or(ServiceRegistrationError::NativeDocument)?,
        exec_main_status: exec_main_status.ok_or(ServiceRegistrationError::NativeDocument)?,
        restarts: restarts.ok_or(ServiceRegistrationError::NativeDocument)?,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<(), ServiceRegistrationError> {
    if slot.replace(value).is_some() {
        Err(ServiceRegistrationError::NativeDocument)
    } else {
        Ok(())
    }
}

fn bounded_systemd_token(value: &str) -> Result<Box<str>, ServiceRegistrationError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ServiceRegistrationError::NativeDocument);
    }
    Ok(value.into())
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
