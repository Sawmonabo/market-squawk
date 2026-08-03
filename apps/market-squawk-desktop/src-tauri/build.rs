fn main() {
    let manifest = tauri_build::AppManifest::new().commands(&[
        "application_invoke",
        "desktop_bootstrap",
        "open_official_provider_page",
        "open_protected_provider_setup",
        "preview_portfolio_import",
        "commit_portfolio_import",
        "discard_portfolio_import",
        "provider_onboarding",
    ]);
    if let Err(error) =
        tauri_build::try_build(tauri_build::Attributes::new().app_manifest(manifest))
    {
        eprintln!("Market Squawk desktop build configuration failed: {error:#}");
        std::process::exit(1);
    }
}
