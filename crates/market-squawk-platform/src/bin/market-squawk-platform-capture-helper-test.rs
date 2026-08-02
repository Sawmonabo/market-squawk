//! Feature-gated process harness for platform capture lifecycle tests.

use std::process::ExitCode;

fn main() -> ExitCode {
    match market_squawk_platform::run_capture_helper(std::env::args_os().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_error) => ExitCode::FAILURE,
    }
}
