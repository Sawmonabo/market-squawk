use std::{env, error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    const SERVICE_INFO_PLIST: &str = "src/bin/market-squawk-service-info.plist";

    println!("cargo::rerun-if-changed={SERVICE_INFO_PLIST}");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return Ok(());
    }

    let plist = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join(SERVICE_INFO_PLIST);
    println!(
        "cargo::rustc-link-arg-bin=market-squawk-service=-Wl,-sectcreate,__TEXT,__info_plist,{}",
        plist.display()
    );
    Ok(())
}
