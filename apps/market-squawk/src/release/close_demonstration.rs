//! Strict semantic validation of the complete offline demonstration report.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context as _, Result, bail};
use rust_decimal::Decimal;
use serde_json::Value;

use super::close::{reject_credentials, string_set};
use super::io::{StableFileIdentity, hash_stable_file, ordered_strings_sha256};
use crate::application::application_capabilities;

const MAXIMUM_REPORT_BYTES: u64 = 64 * 1024 * 1024;
const REQUIRED_APPLICATION_DOMAINS: [&str; 11] = [
    "analysis",
    "bot",
    "execution",
    "fair_value",
    "fundamental",
    "macro",
    "market",
    "model",
    "portfolio",
    "research",
    "source",
];

pub(super) fn validate_demonstration_evidence(
    payload: &Value,
    provider_report: &StableFileIdentity,
    python_directory: &Path,
    application_binary: &StableFileIdentity,
) -> Result<()> {
    if payload.pointer("/schema_version").and_then(Value::as_u64) != Some(1)
        || payload.pointer("/offline").and_then(Value::as_bool) != Some(true)
        || payload.pointer("/completed").and_then(Value::as_bool) != Some(true)
    {
        bail!("release demonstration omitted its terminal offline predicate");
    }
    let python_manifest = hash_stable_file(
        &python_directory.join("market-squawk-release.json"),
        MAXIMUM_REPORT_BYTES,
    )?;
    let python_evidence = hash_stable_file(
        &python_directory.join("market-squawk-release-evidence.json"),
        MAXIMUM_REPORT_BYTES,
    )?;
    for (path, expected) in [
        ("/inputs/provider_report", provider_report),
        ("/inputs/python_release_manifest", &python_manifest),
        ("/inputs/python_release_evidence", &python_evidence),
        ("/inputs/application_binary", application_binary),
    ] {
        if !identity_matches(payload.pointer(path), expected) {
            bail!("release demonstration input identity is incomplete or inconsistent");
        }
    }

    validate_live_kernels(payload)?;
    validate_research_kernels(payload)?;
    validate_local_application(payload)?;
    reject_credentials(payload)
}

fn validate_live_kernels(payload: &Value) -> Result<()> {
    if payload
        .pointer("/production_kernels/coinbase_public_decoder/quality_ceiling")
        .and_then(Value::as_str)
        != Some("direct_unverified")
        || payload
            .pointer("/production_kernels/coinbase_public_decoder/automated_action_eligible")
            .and_then(Value::as_bool)
            != Some(false)
        || !positive(
            payload
                .pointer("/production_kernels/components/kraken_decoder_and_checksum/operations"),
        )
        || !positive(payload.pointer("/production_kernels/components/native_inference/operations"))
        || !positive(payload.pointer("/production_kernels/components/onnx_inference/operations"))
        || payload
            .pointer("/production_kernels/integrated_live_path/dispatch_disposition")
            .and_then(Value::as_str)
            != Some("dispatched")
        || payload
            .pointer("/production_kernels/integrated_live_path/paper_terminal_state")
            .and_then(Value::as_str)
            != Some("filled")
        || payload
            .pointer("/production_kernels/integrated_live_path/paper_order_count")
            .and_then(Value::as_u64)
            != Some(1)
        || !positive(payload.pointer("/production_kernels/integrated_live_path/paper_fill_count"))
        || payload
            .pointer("/production_kernels/integrated_live_path/shutdown_complete")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/production_kernels/coinbase_public_decoder/snapshot_observations")
            .and_then(Value::as_u64)
            != Some(1)
        || payload
            .pointer("/production_kernels/coinbase_public_decoder/delta_observations")
            .and_then(Value::as_u64)
            != Some(1)
        || payload
            .pointer("/production_kernels/coinbase_public_decoder/trade_observations")
            .and_then(Value::as_u64)
            != Some(1)
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/coinbase_public_decoder/snapshot_sha256"),
        )
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/coinbase_public_decoder/delta_sha256"),
        )
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/coinbase_public_decoder/trade_sha256"),
        )
    {
        bail!("release demonstration did not complete the production live/model/risk kernels");
    }
    Ok(())
}

fn validate_research_kernels(payload: &Value) -> Result<()> {
    if payload
        .pointer("/production_kernels/analytical_storage/unique_parquet_objects")
        .and_then(Value::as_u64)
        != Some(1)
        || payload
            .pointer("/production_kernels/analytical_storage/requested_rows")
            .and_then(Value::as_u64)
            != Some(64)
        || payload
            .pointer("/production_kernels/analytical_storage/physical_rows_per_object")
            .and_then(Value::as_u64)
            != Some(64)
        || payload
            .pointer("/production_kernels/analytical_storage/point_in_time_selected_rows")
            .and_then(Value::as_u64)
            != Some(64)
        || payload
            .pointer("/production_kernels/analytical_storage/phase_one_verified_rows")
            .and_then(Value::as_u64)
            != Some(64)
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/analytical_storage/point_in_time_content_sha256"),
        )
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/analytical_storage/point_in_time_audit_sha256"),
        )
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/analytical_storage/phase_one_descriptor_sha256"),
        )
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/analytical_storage/phase_one_manifest_sha256"),
        )
        || !nonzero_digest_array(
            payload.pointer("/production_kernels/analytical_storage/phase_one_object_sha256"),
        )
        || payload
            .pointer("/production_kernels/backtest/fill_count")
            .and_then(Value::as_u64)
            != Some(2)
        || payload
            .pointer("/production_kernels/backtest/filled_lots")
            .and_then(Value::as_i64)
            != Some(4)
        || payload
            .pointer("/production_kernels/backtest/partial_fill_count")
            .and_then(Value::as_u64)
            != Some(2)
        || payload
            .pointer("/production_kernels/backtest/execution_policy_version")
            .and_then(Value::as_u64)
            != Some(3)
        || payload
            .pointer("/production_kernels/backtest/fee_basis_points")
            .and_then(Value::as_i64)
            != Some(10)
        || payload
            .pointer("/production_kernels/backtest/slippage_basis_points")
            .and_then(Value::as_i64)
            != Some(5)
        || payload
            .pointer("/production_kernels/backtest/latency_nanos")
            .and_then(Value::as_u64)
            != Some(1)
        || payload
            .pointer("/production_kernels/backtest/partial_fills_enabled")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/production_kernels/backtest/fee_currency")
            .and_then(Value::as_str)
            != Some("USD")
        || !positive_decimal(payload.pointer("/production_kernels/backtest/fee_amount"))
        || payload
            .pointer("/production_kernels/backtest/accounting_reconciliation")
            .and_then(Value::as_str)
            != Some("independent")
    {
        bail!("release demonstration did not complete analytical/PIT/Python/backtest kernels");
    }
    Ok(())
}

fn validate_local_application(payload: &Value) -> Result<()> {
    let capabilities =
        application_capabilities().context("release application descriptor is invalid")?;
    let expected_names = capabilities
        .tools()
        .iter()
        .map(|tool| tool.name().to_owned())
        .collect::<Vec<_>>();
    let expected_tool_count =
        u64::try_from(expected_names.len()).context("release tool count exceeds u64")?;
    let expected_tool_names_sha256 = ordered_strings_sha256(&expected_names)?;
    let domains = string_set(
        payload
            .pointer("/local_application/capabilities/domains")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("release demonstration omitted application domains"))?,
        "release demonstration application domains",
    )?;
    let expected_domains = REQUIRED_APPLICATION_DOMAINS
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let tool_count = payload
        .pointer("/local_application/capabilities/tool_count")
        .and_then(Value::as_u64);
    if domains != expected_domains
        || tool_count != Some(expected_tool_count)
        || payload
            .pointer("/local_application/capabilities/descriptor_contract_valid")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/local_application/training_environment_admitted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/local_application/model_runtime_composed")
            .and_then(Value::as_bool)
            != Some(true)
    {
        bail!("release demonstration did not compose the complete local application");
    }
    for path in [
        "/local_application/cli/local_file_ingest",
        "/local_application/cli/dataset_manifest",
        "/local_application/cli/datafusion_query",
        "/local_application/cli/source_reads",
        "/local_application/cli/model_registry",
        "/local_application/cli/portfolio_import",
        "/local_application/cli/portfolio_analytics",
        "/local_application/cli/fair_value_no_level1_promotion",
        "/local_application/cli/bot_status",
        "/local_application/cli/stopped_execution_fail_closed",
        "/local_application/doctor/local_storage_unmodified",
        "/local_application/doctor/remote_exporter_disabled",
        "/local_application/doctor/arbitrary_artifact_path_access_disabled",
        "/local_application/mcp/descriptor_parity",
        "/local_application/mcp/read_call_completed",
        "/local_application/mcp/durable_audit_written",
        "/local_application/mcp/shutdown_complete",
        "/local_application/completed",
    ] {
        if payload.pointer(path).and_then(Value::as_bool) != Some(true) {
            bail!("release demonstration omitted a required local application predicate");
        }
    }
    if payload
        .pointer("/local_application/mcp/protocol_version")
        .and_then(Value::as_str)
        != Some("2026-07-28")
        || payload
            .pointer("/local_application/mcp/tool_count")
            .and_then(Value::as_u64)
            != tool_count
        || !digest_array_matches(
            payload.pointer("/local_application/mcp/tool_names_sha256"),
            &expected_tool_names_sha256,
        )
    {
        bail!("release demonstration MCP surface differs from the application descriptor");
    }
    Ok(())
}

fn identity_matches(value: Option<&Value>, expected: &StableFileIdentity) -> bool {
    value
        .and_then(|value| value.pointer("/sha256"))
        .and_then(Value::as_str)
        == Some(expected.sha256.as_str())
        && value
            .and_then(|value| value.pointer("/byte_count"))
            .and_then(Value::as_u64)
            == Some(expected.byte_count)
}

fn positive(value: Option<&Value>) -> bool {
    value.and_then(Value::as_u64).is_some_and(|value| value > 0)
}

fn positive_decimal(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<Decimal>().ok())
        .is_some_and(|value| value > Decimal::ZERO)
}

fn nonzero_digest_array(value: Option<&Value>) -> bool {
    value.and_then(Value::as_array).is_some_and(|bytes| {
        bytes.len() == 32
            && bytes
                .iter()
                .all(|byte| byte.as_u64().is_some_and(|byte| byte <= u64::from(u8::MAX)))
            && bytes.iter().any(|byte| byte.as_u64() != Some(0))
    })
}

fn digest_array_matches(value: Option<&Value>, expected: &[u8; 32]) -> bool {
    value.and_then(Value::as_array).is_some_and(|bytes| {
        bytes.len() == expected.len()
            && bytes.iter().zip(expected).all(|(actual, expected)| {
                actual.as_u64().and_then(|actual| u8::try_from(actual).ok()) == Some(*expected)
            })
    })
}
