//! Relocatable launcher for the sealed Python training product.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    if let Err(message) = run(std::env::args_os().skip(1)) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

fn run(arguments: impl Iterator<Item = OsString>) -> Result<(), &'static str> {
    let launcher = std::env::current_exe()
        .and_then(fs::canonicalize)
        .map_err(|_| "Market Squawk could not resolve the installed training launcher.")?;
    let python = installed_python(&launcher)?;
    admit_regular_executable(&python)?;

    let mut command = Command::new(python);
    command
        .arg("-I")
        .arg("-B")
        .arg("-m")
        .arg("market_squawk.training_driver")
        .args(arguments);

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;

        let error = command.exec();
        Err(match error.kind() {
            std::io::ErrorKind::NotFound => {
                "The installed Market Squawk Python runtime is unavailable."
            }
            _ => "The installed Market Squawk training runtime could not start.",
        })
    }

    #[cfg(windows)]
    {
        let status = command
            .status()
            .map_err(|_| "The installed Market Squawk training runtime could not start.")?;
        if status.success() {
            Ok(())
        } else {
            Err("Market Squawk training did not complete successfully.")
        }
    }
}

fn installed_python(launcher: &Path) -> Result<PathBuf, &'static str> {
    let directory = launcher
        .parent()
        .ok_or("The installed Market Squawk training layout is invalid.")?;
    #[cfg(unix)]
    let python = directory.join("python");
    #[cfg(windows)]
    let python = directory
        .parent()
        .ok_or("The installed Market Squawk training layout is invalid.")?
        .join("python.exe");
    Ok(python)
}

fn admit_regular_executable(path: &Path) -> Result<(), &'static str> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| "The installed Market Squawk Python runtime is unavailable.")?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
        return Err("The installed Market Squawk Python runtime is invalid.");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = metadata.permissions().mode();
        if mode & 0o111 == 0 || mode & 0o022 != 0 {
            return Err("The installed Market Squawk Python runtime has unsafe permissions.");
        }
    }
    Ok(())
}
