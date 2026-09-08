fn main() {
    let manifest = tauri_build::AppManifest::new().commands(&[
        "analysis_control",
        "commit_portfolio_import",
        "commit_research_file_import",
        "dashboard_query",
        "decision_control",
        "desktop_bootstrap",
        "desktop_service_bootstrap",
        "desktop_service_reconnect",
        "discard_portfolio_import",
        "discard_research_file_import",
        "fair_value_control",
        "governance_control",
        "governance_query",
        "import_provider_credential_bundle",
        "installation_control",
        "job_control",
        "mcp_client_control",
        "mcp_status",
        "model_control",
        "operations_control",
        "open_official_provider_page",
        "open_protected_provider_setup",
        "paper_control",
        "preview_portfolio_import",
        "preview_research_file_import",
        "provider_onboarding",
        "research_control",
        "source_control",
        "stage_training_input",
        "start_backtest_from_file",
        "subscribe_service_events",
        "unsubscribe_service_events",
    ]);
    if let Err(error) =
        tauri_build::try_build(tauri_build::Attributes::new().app_manifest(manifest))
    {
        eprintln!("Market Squawk desktop build configuration failed: {error:#}");
        std::process::exit(1);
    }
}
