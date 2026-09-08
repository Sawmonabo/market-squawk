use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_sources::{
    ApiEndpointRule, BackoffPolicy, BudgetDecision, BudgetScope, EndpointPolicy, HttpClientProfile,
    HttpRequestBounds, NetworkPolicyError, PathScope, ProviderBudgetPolicy, QueryParameterRule,
    QuerySensitivity, RetryAfter, install_ring_tls_provider,
};

use crate::common::{TestResult, source_identifier};

#[test]
fn rustls_provider_installation_is_explicit_and_project_idempotent() -> TestResult {
    let first = install_ring_tls_provider()?;
    assert_eq!(first.provider_id(), "rustls-ring-0.23.42");
    let second = install_ring_tls_provider()?;
    assert_eq!(second.provider_id(), first.provider_id());
    Ok(())
}

#[test]
fn redirect_must_remain_allowlisted() -> TestResult {
    let policy = EndpointPolicy::try_new(["wss://advanced-trade-ws.coinbase.com"])?;
    assert!(matches!(
        policy.authorize_redirect("https://attacker.invalid/frame"),
        Err(NetworkPolicyError::EndpointDenied { .. })
    ));
    Ok(())
}

#[test]
fn dynamic_api_rules_are_segment_and_query_structural() -> TestResult {
    let api_key = QueryParameterRule::try_new(
        source_identifier("api_key")?,
        64,
        false,
        QuerySensitivity::Secret,
    )?;
    let series = QueryParameterRule::try_new(
        source_identifier("series_id")?,
        64,
        false,
        QuerySensitivity::Public,
    )?;
    let rule = ApiEndpointRule::try_new(
        "https://api.stlouisfed.org/fred",
        PathScope::Descendants,
        vec![api_key, series],
        4,
        512,
    )?;
    let policy = EndpointPolicy::try_from_api_rules(vec![rule], HttpRequestBounds::default())?;
    let secret = "do-not-retain-this-secret";
    let authorized = policy.authorize_request(&format!(
        "https://api.stlouisfed.org/fred/series?series_id=GDP&api_key={secret}"
    ))?;
    assert!(authorized.contains_sensitive_query());
    assert!(!format!("{authorized:?}").contains(secret));
    for denied in [
        "https://api.stlouisfed.org/fredevil?series_id=GDP",
        "https://api.stlouisfed.org/fred/%2e%2e/admin?series_id=GDP",
        "https://api.stlouisfed.org/fred/series?unknown=GDP",
        "https://api.stlouisfed.org/fred/series?series_id=GDP&series_id=CPI",
    ] {
        assert!(matches!(
            policy.authorize_request(denied),
            Err(NetworkPolicyError::EndpointDenied { .. })
        ));
    }

    let required_empty =
        QueryParameterRule::try_new_exact_empty_public(source_identifier("lastObs")?)?;
    let exact_format = QueryParameterRule::try_new_exact_public(
        source_identifier("format")?,
        source_identifier("json")?,
    )?;
    let rule = ApiEndpointRule::try_new(
        "https://api.example.test/history",
        PathScope::Exact,
        vec![required_empty, exact_format],
        3,
        64,
    )?;
    let policy = EndpointPolicy::try_from_api_rules(vec![rule], HttpRequestBounds::default())?;
    let wire = serde_json::to_string(&policy)?;
    let policy: EndpointPolicy = serde_json::from_str(&wire)?;
    policy.authorize_request("https://api.example.test/history?lastObs=&format=json")?;
    for denied in [
        "https://api.example.test/history?format=json",
        "https://api.example.test/history?LastObs=&format=json",
        "https://api.example.test/history?lastObs=value&format=json",
        "https://api.example.test/history?lastObs=&lastObs=&format=json",
    ] {
        assert!(matches!(
            policy.authorize_request(denied),
            Err(NetworkPolicyError::EndpointDenied { .. })
        ));
    }
    let invalid_wire = wire.replace("\"max_value_bytes\":0", "\"max_value_bytes\":1");
    assert_ne!(invalid_wire, wire);
    assert!(serde_json::from_str::<EndpointPolicy>(&invalid_wire).is_err());
    Ok(())
}

#[test]
fn endpoint_wire_and_client_profile_fail_closed() -> TestResult {
    let policy = EndpointPolicy::try_new(["https://[::1]/api"])?;
    let wire = serde_json::to_string(&policy)?;
    let restored: EndpointPolicy = serde_json::from_str(&wire)?;
    restored.authorize("https://[::1]/api")?;
    restored.authorize("https://[::1]:443/api")?;
    let profile = restored.client_profile();
    assert_eq!(profile, HttpClientProfile::hardened());
    assert!(profile.automatic_redirects_disabled());
    assert!(profile.ambient_system_proxy_disabled());
    assert!(profile.implicit_retries_disabled());
    assert!(profile.counts_post_decompression_bytes());
    let tampered = wire.replace(
        "\"automatic_redirects\":false",
        "\"automatic_redirects\":true",
    );
    assert!(serde_json::from_str::<EndpointPolicy>(&tampered).is_err());
    Ok(())
}

#[test]
fn ambiguous_raw_urls_are_rejected_before_url_normalization() -> TestResult {
    let rule = ApiEndpointRule::try_new(
        "https://api.example.test/v1",
        PathScope::Descendants,
        Vec::new(),
        1,
        32,
    )?;
    let policy = EndpointPolicy::try_from_api_rules(vec![rule], HttpRequestBounds::default())?;
    for denied in [
        "https://api.example.test/v1/../admin",
        "https://api.example.test/v1/%2e%2E/admin",
        "https://api.example.test/v1/%252e%252e/admin",
        r"https://api.example.test\v1\admin",
        "https://user@api.example.test/v1",
        "https://api.example.test/v1\n/admin",
        "https://api.example.test/v1 /admin",
    ] {
        assert!(matches!(
            policy.authorize_request(denied),
            Err(NetworkPolicyError::EndpointDenied { .. })
        ));
    }
    Ok(())
}

#[test]
fn invalid_backoff_wire_is_rejected() -> TestResult {
    let invalid = r#"{"initial_nanos":10,"maximum_nanos":1,"jitter_basis_points":10001}"#;
    assert!(serde_json::from_str::<BackoffPolicy>(invalid).is_err());
    Ok(())
}

#[test]
fn budget_policy_has_no_rotation_or_evasion_surface() -> TestResult {
    let policy = ProviderBudgetPolicy::try_new(
        BudgetScope::with_authorization_account(
            source_identifier("fred")?,
            source_identifier("user-account-reference")?,
        ),
        NonZeroU32::try_from(2_u32)?,
        NonZeroU64::try_from(1_000_000_000_u64)?,
        NonZeroU16::try_from(1_u16)?,
        BackoffPolicy::try_new(
            NonZeroU64::try_from(1_000_u64)?,
            NonZeroU64::try_from(1_000_000_000_u64)?,
            500,
        )?,
    )?;
    let wire = serde_json::to_string(&policy)?;
    for forbidden in ["proxy", "fingerprint", "captcha", "rotation", "shard"] {
        assert!(!wire.to_ascii_lowercase().contains(forbidden));
    }
    let _typed = RetryAfter::Delay(NonZeroU64::try_from(1_u64)?);
    let _decision_type: Option<BudgetDecision> = None;
    Ok(())
}
