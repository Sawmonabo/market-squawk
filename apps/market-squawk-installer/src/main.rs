use std::process::ExitCode;

fn main() -> ExitCode {
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => match runtime.block_on(market_squawk_installer::run_cli()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("market-squawk-installer: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("market-squawk-installer: failed to start the async runtime: {error}");
            ExitCode::FAILURE
        }
    }
}
