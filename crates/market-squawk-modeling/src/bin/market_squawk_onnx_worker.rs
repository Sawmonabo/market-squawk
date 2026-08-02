//! Private Market Squawk ONNX helper-process entry point.

fn main() -> std::process::ExitCode {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--stdio-worker"))
        || arguments.next().is_some()
    {
        return std::process::ExitCode::FAILURE;
    }
    match market_squawk_modeling::run_onnx_worker_process() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}
