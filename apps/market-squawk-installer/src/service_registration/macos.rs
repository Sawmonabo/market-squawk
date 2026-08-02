//! macOS 12+ per-user LaunchAgent registration.

#![cfg_attr(
    test,
    expect(
        dead_code,
        reason = "library tests validate rendered LaunchAgent semantics without invoking launchctl"
    )
)]

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use directories::BaseDirs;

use super::{
    NativeRegistrationSnapshot, PreparedRegistration, REGISTRATION_OWNER, ServiceRegistrationError,
    native_document, run_bounded, sha256_bytes, xml_escape,
};

pub(super) const REGISTRATION_IDENTITY: &str = "com.marketsquawk.service";
const LAUNCHCTL: &str = "/bin/launchctl";

pub(super) fn render_launch_agent(
    service: &Path,
    release_root: &Path,
) -> Result<String, ServiceRegistrationError> {
    let service = service.to_str().ok_or(ServiceRegistrationError::Identity)?;
    let release_root = release_root
        .to_str()
        .ok_or(ServiceRegistrationError::Identity)?;
    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <!-- owner:{owner} -->\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
           <key>Label</key>\n\
           <string>{label}</string>\n\
           <key>ProgramArguments</key>\n\
           <array>\n\
             <string>{service}</string>\n\
             <string>--training-release-root</string>\n\
             <string>{release_root}</string>\n\
           </array>\n\
           <key>RunAtLoad</key>\n\
           <true/>\n\
           <key>KeepAlive</key>\n\
           <dict>\n\
             <key>SuccessfulExit</key>\n\
             <false/>\n\
           </dict>\n\
           <key>ProcessType</key>\n\
           <string>Background</string>\n\
           <key>ThrottleInterval</key>\n\
           <integer>5</integer>\n\
           <key>Umask</key>\n\
           <integer>63</integer>\n\
         </dict>\n\
         </plist>\n",
        owner = REGISTRATION_OWNER,
        label = xml_escape(REGISTRATION_IDENTITY)?,
        service = xml_escape(service)?,
        release_root = xml_escape(release_root)?,
    ))
}

pub(super) fn prepare(
    service: &Path,
    release_root: &Path,
) -> Result<PreparedRegistration, ServiceRegistrationError> {
    let document = native_document(render_launch_agent(service, release_root)?.into_bytes())?;
    Ok(PreparedRegistration {
        identity: REGISTRATION_IDENTITY,
        configuration_sha256: sha256_bytes(&document),
        document,
    })
}

pub(super) fn inspect() -> Result<Option<NativeRegistrationSnapshot>, ServiceRegistrationError> {
    let path = launch_agent_path()?;
    let Some(document) = read_registration_file(&path)? else {
        return Ok(None);
    };
    let text =
        std::str::from_utf8(&document).map_err(|_| ServiceRegistrationError::NativeDocument)?;
    let owned = text.contains(&format!("<!-- owner:{REGISTRATION_OWNER} -->"))
        && text.contains(&format!("<string>{REGISTRATION_IDENTITY}</string>"));
    Ok(Some(NativeRegistrationSnapshot {
        configuration_sha256: sha256_bytes(&document),
        document,
        owned,
    }))
}

pub(super) fn apply(prepared: &PreparedRegistration) -> Result<(), ServiceRegistrationError> {
    if prepared.identity != REGISTRATION_IDENTITY {
        return Err(ServiceRegistrationError::Identity);
    }
    unload_if_loaded()?;
    write_registration_file(&launch_agent_path()?, &prepared.document)?;
    bootstrap()
}

pub(super) fn start() -> Result<(), ServiceRegistrationError> {
    let target = service_target()?;
    run_bounded(
        Path::new(LAUNCHCTL),
        [OsString::from("enable"), target.clone()],
        false,
    )?;
    run_bounded(
        Path::new(LAUNCHCTL),
        [OsString::from("kickstart"), target],
        false,
    )?;
    Ok(())
}

pub(super) fn restart() -> Result<(), ServiceRegistrationError> {
    run_bounded(
        Path::new(LAUNCHCTL),
        [
            OsString::from("kickstart"),
            OsString::from("-k"),
            service_target()?,
        ],
        false,
    )?;
    Ok(())
}

pub(super) fn ensure_active() -> Result<(), ServiceRegistrationError> {
    run_bounded(
        Path::new(LAUNCHCTL),
        [OsString::from("print"), service_target()?],
        false,
    )?;
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
    unload_if_loaded()?;
    fs::remove_file(launch_agent_path()?)
        .map_err(|source| ServiceRegistrationError::io("remove LaunchAgent", source))?;
    sync_parent(&launch_agent_path()?)
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
        unload_if_loaded()?;
    }
    match prior {
        Some(prior) if prior.owned => {
            write_registration_file(&launch_agent_path()?, &prior.document)?;
            bootstrap()?;
            start()
        }
        Some(_) => Err(ServiceRegistrationError::Conflict),
        None => {
            let path = launch_agent_path()?;
            match fs::remove_file(&path) {
                Ok(()) => sync_parent(&path),
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(source) => Err(ServiceRegistrationError::io(
                    "remove failed LaunchAgent",
                    source,
                )),
            }
        }
    }
}

fn bootstrap() -> Result<(), ServiceRegistrationError> {
    run_bounded(
        Path::new(LAUNCHCTL),
        [
            OsString::from("bootstrap"),
            user_domain()?,
            launch_agent_path()?.into_os_string(),
        ],
        false,
    )?;
    Ok(())
}

fn unload_if_loaded() -> Result<(), ServiceRegistrationError> {
    match run_bounded(
        Path::new(LAUNCHCTL),
        [OsString::from("bootout"), service_target()?],
        false,
    ) {
        Ok(_) | Err(ServiceRegistrationError::CommandFailed(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn user_domain() -> Result<OsString, ServiceRegistrationError> {
    let user = rustix::process::geteuid().as_raw();
    Ok(OsString::from(format!("gui/{user}")))
}

fn service_target() -> Result<OsString, ServiceRegistrationError> {
    let domain = user_domain()?;
    Ok(OsString::from(format!(
        "{}/{}",
        domain.to_string_lossy(),
        REGISTRATION_IDENTITY
    )))
}

fn launch_agent_path() -> Result<PathBuf, ServiceRegistrationError> {
    let base = BaseDirs::new().ok_or(ServiceRegistrationError::UnsafePath)?;
    Ok(base
        .home_dir()
        .join("Library/LaunchAgents")
        .join(format!("{REGISTRATION_IDENTITY}.plist")))
}

fn read_registration_file(path: &Path) -> Result<Option<Vec<u8>>, ServiceRegistrationError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ServiceRegistrationError::io("inspect LaunchAgent", source));
        }
    };
    validate_owned_file(&metadata)?;
    let bytes = fs::read(path)
        .map_err(|source| ServiceRegistrationError::io("read LaunchAgent", source))?;
    native_document(bytes).map(Some)
}

fn write_registration_file(path: &Path, bytes: &[u8]) -> Result<(), ServiceRegistrationError> {
    native_document(bytes.to_vec())?;
    let parent = path.parent().ok_or(ServiceRegistrationError::UnsafePath)?;
    fs::create_dir_all(parent)
        .map_err(|source| ServiceRegistrationError::io("create LaunchAgent directory", source))?;
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
            ServiceRegistrationError::io("publish LaunchAgent", source)
        })?;
    sync_parent(path)
}

fn validate_owned_directory(path: &Path) -> Result<(), ServiceRegistrationError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = fs::symlink_metadata(path)
        .map_err(|source| ServiceRegistrationError::io("inspect LaunchAgent directory", source))?;
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
        .map_err(|source| ServiceRegistrationError::io("synchronize LaunchAgent directory", source))
}
