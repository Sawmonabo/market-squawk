//! Strict provider-report predicates used by exact-head closure and demonstration admission.

use std::collections::BTreeSet;

use anyhow::{Result, bail};
use serde_json::Value;

use super::close::string_set;
use super::io::StableFileIdentity;

const REQUIRED_PROVIDER_SURFACES: [&str; 8] = [
    "coinbase.public-market-data",
    "coinbase.exchange-direct-market-data",
    "kraken.spot-public-market-data",
    "sec.edgar-public",
    "fred-alfred.api-v1-v2",
    "bls.v1-unregistered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
];
const ALLOWED_PROVIDER_SURFACES: [&str; 9] = [
    "coinbase.public-market-data",
    "coinbase.exchange-direct-market-data",
    "kraken.spot-public-market-data",
    "sec.edgar-public",
    "fred-alfred.api-v1-v2",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
];

pub(super) fn validate_provider_evidence(payload: &Value) -> Result<()> {
    if payload.pointer("/schema_version").and_then(Value::as_u64) != Some(1)
        || payload
            .pointer("/requirements/external_network_authorized")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/requirements/provider_terms_accepted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/requirements/direct_verified_action_required")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/requirements/fred_alfred_rights_required")
            .and_then(Value::as_bool)
            != Some(true)
    {
        bail!("provider release evidence omitted a required acceptance gate");
    }

    let selected = string_set(
        payload
            .pointer("/selected_surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("provider evidence omitted selected surfaces"))?,
        "provider selected surfaces",
    )?;
    if selected
        .iter()
        .any(|surface| !ALLOWED_PROVIDER_SURFACES.contains(&surface.as_str()))
        || REQUIRED_PROVIDER_SURFACES
            .iter()
            .any(|required| !selected.contains(*required))
    {
        bail!("provider evidence does not contain the closed mandatory surface set");
    }

    let recovered = string_set(
        payload
            .pointer("/restart_recovery/recovered_surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("provider evidence omitted restart recovery"))?,
        "provider recovered surfaces",
    )?;
    if payload
        .pointer("/restart_recovery/completed")
        .and_then(Value::as_bool)
        != Some(true)
        || recovered != selected
    {
        bail!("provider evidence did not recover every selected surface after restart");
    }

    let surfaces = payload
        .pointer("/surfaces")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("provider evidence omitted surface records"))?;
    let mut represented = BTreeSet::new();
    let mut observed_direct_orders = None;
    for surface in surfaces {
        let surface_id = surface
            .pointer("/surface_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("provider surface evidence omitted its identity"))?;
        if !selected.contains(surface_id) || !represented.insert(surface_id.to_owned()) {
            bail!("provider surface evidence is duplicated or outside the selected set");
        }
        if surface.pointer("/session/state").and_then(Value::as_str) != Some("active_scoped")
            || !nonzero_evidence_digest(
                surface
                    .pointer("/activation/capability_digest")
                    .ok_or_else(|| anyhow::anyhow!("provider capability digest is absent"))?,
            )
            || !nonzero_evidence_digest(
                surface
                    .pointer("/activation/rights_decision_digest")
                    .ok_or_else(|| anyhow::anyhow!("provider rights digest is absent"))?,
            )
            || !nonzero_evidence_digest(
                surface
                    .pointer("/activation/runtime_response_digest")
                    .ok_or_else(|| anyhow::anyhow!("provider runtime receipt is absent"))?,
            )
        {
            bail!("provider surface evidence omitted active immutable authority");
        }
        validate_provider_surface_runtime(surface_id, surface)?;
        if surface_id == "coinbase.exchange-direct-market-data" {
            observed_direct_orders = surface
                .pointer("/live_runtime/orders")
                .and_then(Value::as_array)
                .map(std::vec::Vec::len);
        }
    }
    if represented != selected {
        bail!("provider surface records do not exactly match the selected set");
    }

    if payload
        .pointer("/direct_verified_action/required")
        .and_then(Value::as_bool)
        != Some(true)
        || payload
            .pointer("/direct_verified_action/selected")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/direct_verified_action/completed")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/direct_verified_action/order_count")
            .and_then(Value::as_u64)
            .and_then(|count| usize::try_from(count).ok())
            .is_none_or(|count| count == 0 || Some(count) != observed_direct_orders)
    {
        bail!("provider evidence omitted the required DirectVerified paper action");
    }
    if payload
        .pointer("/fred_alfred_rights/required")
        .and_then(Value::as_bool)
        != Some(true)
        || payload
            .pointer("/fred_alfred_rights/selected")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/fred_alfred_rights/persistence_admitted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/fred_alfred_rights/model_training_admitted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/fred_alfred_rights/admitted")
            .and_then(Value::as_bool)
            != Some(true)
    {
        bail!("provider evidence omitted admitted FRED and ALFRED durable-use rights");
    }
    Ok(())
}

fn validate_provider_surface_runtime(surface_id: &str, surface: &Value) -> Result<()> {
    match surface_id {
        "coinbase.public-market-data" | "kraken.spot-public-market-data" => {
            let live = surface
                .pointer("/live_runtime")
                .ok_or_else(|| anyhow::anyhow!("public live-provider evidence is absent"))?;
            if live.pointer("/expected_quality").and_then(Value::as_str)
                != Some("direct_unverified")
                || surface
                    .pointer("/live_runtime/action_completed")
                    .and_then(Value::as_bool)
                    != Some(false)
                || !empty_result(live.pointer("/orders"))
                || !valid_live_source_evidence(live, surface_id, "direct_unverified")
            {
                bail!("public live-provider evidence violated its quality/action boundary");
            }
        }
        "coinbase.exchange-direct-market-data" => {
            let live = surface
                .pointer("/live_runtime")
                .ok_or_else(|| anyhow::anyhow!("Coinbase Direct live evidence is absent"))?;
            if live.pointer("/expected_quality").and_then(Value::as_str) != Some("direct_verified")
                || surface
                    .pointer("/live_runtime/action_completed")
                    .and_then(Value::as_bool)
                    != Some(true)
                || live
                    .pointer("/orders")
                    .and_then(Value::as_array)
                    .is_none_or(std::vec::Vec::is_empty)
                || !valid_live_source_evidence(live, surface_id, "direct_verified")
            {
                bail!("Coinbase Direct evidence omitted verified action authority");
            }
        }
        "sec.edgar-public"
        | "fred-alfred.api-v1-v2"
        | "bls.v1-unregistered"
        | "bls.v2-registered"
        | "treasury.fiscal-data" => {
            let runtime = surface
                .pointer("/research_runtime")
                .filter(|runtime| !runtime.is_null())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "durable research-provider evidence omitted its callable runtime"
                    )
                })?;
            if !nonzero_evidence_digest(
                runtime
                    .pointer("/runtime_generation_digest")
                    .ok_or_else(|| anyhow::anyhow!("research runtime digest is absent"))?,
            ) || !nonzero_evidence_digest(
                runtime
                    .pointer("/rights_authorization_digest")
                    .ok_or_else(|| anyhow::anyhow!("research rights digest is absent"))?,
            ) {
                bail!("durable research-provider runtime evidence is invalid");
            }
            if surface_id == "fred-alfred.api-v1-v2"
                && (surface
                    .pointer("/activation/data_use_admission/persist")
                    .and_then(Value::as_bool)
                    != Some(true)
                    || surface
                        .pointer("/activation/data_use_admission/model_training")
                        .and_then(Value::as_bool)
                        != Some(true))
            {
                bail!("FRED/ALFRED runtime lacks admitted durable-use operations");
            }
        }
        "treasury.daily-rates-xml" => {}
        _ => bail!("provider evidence contains an unknown surface"),
    }
    Ok(())
}

fn empty_result(value: Option<&Value>) -> bool {
    value.is_some_and(|value| {
        value.is_null() || value.as_array().is_some_and(std::vec::Vec::is_empty)
    })
}

fn valid_live_source_evidence(live: &Value, surface_id: &str, expected_quality: &str) -> bool {
    let status = live.pointer("/source_status").and_then(Value::as_array);
    let coverage = live.pointer("/source_coverage").and_then(Value::as_array);
    let health = live.pointer("/source_health").and_then(Value::as_array);
    status.is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer("/profile/id").and_then(Value::as_str) == Some(surface_id)
                    && row.pointer("/runtime/state").and_then(Value::as_str) == Some("active")
                    && row.pointer("/runtime/quality").and_then(Value::as_str)
                        == Some(expected_quality)
            })
    }) && coverage.is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer("/surfaceId").and_then(Value::as_str) == Some(surface_id)
                    && row
                        .pointer("/runtimeCoverage/state")
                        .and_then(Value::as_str)
                        == Some("established")
            })
    }) && health.is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer("/surfaceId").and_then(Value::as_str) == Some(surface_id)
                    && row.pointer("/runtimeHealth/state").and_then(Value::as_str) == Some("active")
                    && row
                        .pointer("/runtimeHealth/quality")
                        .and_then(Value::as_str)
                        == Some(expected_quality)
            })
    })
}

pub(super) fn validate_provider_binary(payload: &Value, binary: &StableFileIdentity) -> Result<()> {
    if payload
        .pointer("/executable/sha256")
        .and_then(Value::as_str)
        != Some(binary.sha256.as_str())
        || payload
            .pointer("/executable/byte_count")
            .and_then(Value::as_u64)
            != Some(binary.byte_count)
    {
        bail!("provider evidence does not bind the exact release executable");
    }
    Ok(())
}

fn nonzero_evidence_digest(value: &Value) -> bool {
    value.pointer("/algorithm").and_then(Value::as_str) == Some("sha256")
        && value
            .pointer("/bytes")
            .and_then(Value::as_array)
            .is_some_and(|bytes| {
                bytes.len() == 32
                    && bytes
                        .iter()
                        .all(|byte| byte.as_u64().is_some_and(|byte| byte <= u64::from(u8::MAX)))
                    && bytes.iter().any(|byte| byte.as_u64() != Some(0))
            })
}
