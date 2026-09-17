#[derive(Debug)]
struct TestExtractionAdapter {
    metadata: crate::SourceMetadata,
}

impl crate::SourceMetadataProvider for TestExtractionAdapter {
    fn metadata(&self) -> &crate::SourceMetadata {
        &self.metadata
    }
}

#[test]
fn extraction_authority_is_exact_revocation_aware_and_budget_bound() -> TestResult {
    let at = Timestamp::from_unix_nanos(1_000_000_000);
    let mut registry =
        AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let metadata = extraction_metadata("macro-source", "revision-1", 1)?;
    let adapter = TestExtractionAdapter {
        metadata: metadata.clone(),
    };
    let registered = registry.register(metadata, at)?;
    let authority = registry.extraction_authority(&registered, &adapter)?;

    let permit = authority.try_network_request("https://api.source.test/data")?;
    assert!(!permit.authorization().contains_sensitive_query());
    let in_flight = permit.authorize_send("https://api.source.test/data")?;
    assert!(matches!(
        authority.try_network_request("https://attacker.invalid/data"),
        Err(crate::ExtractionAuthorityError::NetworkPolicy(_))
    ));
    assert!(matches!(
        authority.try_network_request("https://api.source.test/data"),
        Err(crate::ExtractionAuthorityError::BudgetWaitUntil { .. })
    ));

    let replacement_metadata = extraction_metadata("macro-source", "revision-2", 1)?;
    let replacement_adapter = TestExtractionAdapter {
        metadata: replacement_metadata.clone(),
    };
    let replacement = registry.replace_metadata(&registered, replacement_metadata, at)?;
    assert_eq!(
        authority.validate_current(),
        Err(crate::ExtractionAuthorityError::NotCurrent)
    );
    assert_eq!(
        in_flight.validate_current(),
        Err(crate::ExtractionAuthorityError::NotCurrent)
    );
    assert!(matches!(
        registry.extraction_authority(&replacement, &adapter),
        Err(RegistryError::AdapterMetadataMismatch)
    ));

    let replacement_authority =
        registry.extraction_authority(&replacement, &replacement_adapter)?;
    registry.revoke(&replacement, at)?;
    assert_eq!(
        replacement_authority.validate_current(),
        Err(crate::ExtractionAuthorityError::NotCurrent)
    );
    Ok(())
}

#[test]
fn extraction_authority_fails_closed_when_registry_is_dropped() -> TestResult {
    let at = Timestamp::from_unix_nanos(1_000_000_000);
    let metadata = extraction_metadata("drop-source", "revision-1", 1)?;
    let adapter = TestExtractionAdapter {
        metadata: metadata.clone(),
    };
    let authority = {
        let mut registry =
            AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
        let registered = registry.register(metadata, at)?;
        registry.extraction_authority(&registered, &adapter)?
    };

    assert_eq!(
        authority.validate_current(),
        Err(crate::ExtractionAuthorityError::NotCurrent)
    );
    Ok(())
}

#[test]
fn provider_backoff_authority_is_narrow_and_revocation_aware() -> TestResult {
    let at = Timestamp::from_unix_nanos(1_000_000_000);
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let metadata = direct_metadata("backoff-source", "revision-1")?;
    let registered = registry.register(metadata, at)?;
    let authority = registry.provider_backoff_authority(&registered)?;

    let deadline = match authority.apply_refusal(0)? {
        crate::ProviderBackoffDecision::WaitUntil(deadline) => deadline,
        crate::ProviderBackoffDecision::Unavailable(reason) => {
            return Err(format!("provider backoff unexpectedly unavailable: {reason:?}").into());
        }
    };
    let _remaining = authority.remaining_wait(deadline)?;

    let replacement = direct_metadata("backoff-source", "revision-2")?;
    let _current = registry.replace_metadata(&registered, replacement, at)?;
    assert_eq!(
        authority.remaining_wait(deadline),
        Err(crate::ProviderBackoffError::NotCurrent)
    );
    Ok(())
}

#[test]
fn redirect_hops_are_origin_bound_and_each_consume_one_request_admission() -> TestResult {
    let at = Timestamp::from_unix_nanos(1_000_000_000);
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let mut metadata_wire = serde_json::to_value(extraction_metadata(
        "redirect-source",
        "revision-1",
        4,
    )?)?;
    metadata_wire["network"]["allowlisted"]["endpoints"] = serde_json::json!([
        "https://redirect-source.example.test/data",
        "https://redirect-source.example.test/next",
        "https://other-redirect-source.example.test/data"
    ]);
    let metadata: crate::SourceMetadata = serde_json::from_value(metadata_wire)?;
    let adapter = TestExtractionAdapter {
        metadata: metadata.clone(),
    };
    let registered = registry.register(metadata, at)?;
    let authority = registry.extraction_authority(&registered, &adapter)?;

    let cross_origin = authority
        .try_network_request("https://redirect-source.example.test/data")?
        .authorize_send("https://redirect-source.example.test/data")?;
    assert!(matches!(
        cross_origin.authorize_redirect_from(
            "https://redirect-source.example.test/data",
            "https://other-redirect-source.example.test/data",
            true,
        ),
        Err(crate::ExtractionAuthorityError::NetworkPolicy(
            crate::NetworkPolicyError::EndpointDenied { .. }
        ))
    ));

    let first_hop = authority
        .try_network_request("https://redirect-source.example.test/data")?
        .authorize_send("https://redirect-source.example.test/data")?;
    let redirect = first_hop.authorize_redirect_from(
        "https://redirect-source.example.test/data",
        "https://redirect-source.example.test/next",
        true,
    )?;
    assert!(redirect
        .redirect_authorization()
        .forward_sensitive_headers());
    assert!(matches!(
        authority.try_network_request("https://redirect-source.example.test/data"),
        Err(crate::ExtractionAuthorityError::BudgetUnavailable {
            reason: crate::BudgetUnavailableReason::ConcurrencyExhausted
        })
    ));
    assert!(matches!(
        redirect.authorize_send("https://redirect-source.example.test/data"),
        Err(crate::ExtractionAuthorityError::RequestTargetMismatch)
    ));

    authority
        .try_network_request("https://redirect-source.example.test/data")?
        .release();
    assert!(matches!(
        authority.try_network_request("https://redirect-source.example.test/data"),
        Err(crate::ExtractionAuthorityError::BudgetWaitUntil { .. })
    ));
    Ok(())
}

#[test]
fn in_flight_refusal_applies_shared_bounded_retry_after_without_budget_access() -> TestResult {
    let at = Timestamp::from_unix_nanos(1_000_000_000);
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let mut metadata_wire = serde_json::to_value(extraction_metadata(
        "retry-after-source",
        "revision-1",
        4,
    )?)?;
    metadata_wire["network"]["allowlisted"]["endpoints"] =
        serde_json::json!(["https://retry-after-source.example.test/data"]);
    metadata_wire["budget"]["max_concurrent"] = serde_json::json!(2);
    let metadata: crate::SourceMetadata = serde_json::from_value(metadata_wire)?;
    let adapter = TestExtractionAdapter {
        metadata: metadata.clone(),
    };
    let registered = registry.register(metadata, at)?;
    let authority = registry.extraction_authority(&registered, &adapter)?;
    let in_flight = authority
        .try_network_request("https://retry-after-source.example.test/data")?
        .authorize_send("https://retry-after-source.example.test/data")?;
    let successful = authority
        .try_network_request("https://retry-after-source.example.test/data")?
        .authorize_send("https://retry-after-source.example.test/data")?;

    let deadline = in_flight.apply_retry_after_header(Some(b"2"), 0)?;
    assert_eq!(successful.record_success(), Ok(()));
    assert!(matches!(
        authority.try_network_request("https://retry-after-source.example.test/data"),
        Err(crate::ExtractionAuthorityError::BudgetWaitUntil { deadline: actual })
            if actual == deadline
    ));
    Ok(())
}
