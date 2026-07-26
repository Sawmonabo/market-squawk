//! Strict provider-report predicates used by exact-head closure and demonstration admission.

use std::collections::BTreeSet;

use anyhow::{Result, bail};
use market_squawk_adapter_treasury::{TreasuryDailyRateFamily, TreasuryDailyRateQuery};
use market_squawk_sources::DataUseOperation;
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
const DATA_USE_OPERATIONS: [DataUseOperation; 6] = [
    DataUseOperation::Retrieve,
    DataUseOperation::Display,
    DataUseOperation::Persist,
    DataUseOperation::ModelTraining,
    DataUseOperation::Export,
    DataUseOperation::Redistribute,
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
        | "treasury.daily-rates-xml"
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
            if surface_id == "treasury.daily-rates-xml"
                && DATA_USE_OPERATIONS.iter().any(|operation| {
                    surface
                        .pointer(&format!(
                            "/activation/data_use_admission/{}",
                            operation.evidence_name()
                        ))
                        .and_then(Value::as_bool)
                        != Some(true)
                })
            {
                bail!("Treasury daily-rate runtime lacks admitted durable-use operations");
            }
            if surface_id == "treasury.daily-rates-xml" {
                validate_treasury_publications(runtime)?;
            } else if runtime
                .pointer("/publications")
                .and_then(Value::as_array)
                .is_none_or(|publications| !publications.is_empty())
            {
                bail!("non-Treasury research runtime contains unexpected publication evidence");
            }
        }
        _ => bail!("provider evidence contains an unknown surface"),
    }
    Ok(())
}

fn validate_treasury_publications(runtime: &Value) -> Result<()> {
    let publications = runtime
        .pointer("/publications")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Treasury publication evidence is absent"))?;
    if publications.len() != TreasuryDailyRateFamily::ALL.len() {
        bail!("Treasury publication evidence does not cover all five families");
    }
    let mut families = BTreeSet::new();
    let mut provider_datasets = BTreeSet::new();
    let mut analytical_datasets = BTreeSet::new();
    let mut acceptance_year = None;
    for publication in publications {
        let family_name = publication
            .get("family")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Treasury publication family is invalid"))?;
        let family = treasury_family(family_name)
            .ok_or_else(|| anyhow::anyhow!("Treasury publication family is invalid"))?;
        let provider_dataset = publication
            .get("provider_dataset")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Treasury provider dataset is invalid"))?;
        let year = treasury_dataset_year(family, provider_dataset)
            .ok_or_else(|| anyhow::anyhow!("Treasury provider dataset is invalid"))?;
        if acceptance_year
            .replace(year)
            .is_some_and(|other| other != year)
        {
            bail!("Treasury publications do not use one common configured acceptance year");
        }
        let query = TreasuryDailyRateQuery::year(family, year)?;
        if query.dataset().as_str() != provider_dataset {
            bail!("Treasury publication family is not bound to its canonical dataset");
        }
        let source_object = publication
            .get("source_object_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Treasury source object identity is invalid"))?;
        let payload_digest = publication
            .get("source_payload_digest")
            .and_then(evidence_digest_hex)
            .ok_or_else(|| anyhow::anyhow!("Treasury source payload digest is absent"))?;
        let request = query.page(0)?;
        if !treasury_source_object_matches(source_object, request.request_digest(), &payload_digest)
        {
            bail!("Treasury source object is not bound to its dataset and exact payload");
        }
        let analytical_dataset = publication
            .get("analytical_dataset_id")
            .and_then(Value::as_str)
            .filter(|identity| !identity.is_empty())
            .ok_or_else(|| anyhow::anyhow!("Treasury analytical dataset identity is invalid"))?;
        if source_object.is_empty()
            || !families.insert(family)
            || !provider_datasets.insert(provider_dataset)
            || !analytical_datasets.insert(analytical_dataset)
            || publication
                .get("manifest_version")
                .and_then(Value::as_u64)
                .is_none_or(|value| value == 0)
            || !valid_sha256(publication.get("manifest_content_hash"))
            || publication
                .get("row_count")
                .and_then(Value::as_u64)
                .is_none_or(|value| value == 0)
            || publication
                .get("total_bytes")
                .and_then(Value::as_u64)
                .is_none_or(|value| value == 0)
            || publication.get("object_count").and_then(Value::as_u64) != Some(1)
            || !valid_sha256(publication.get("lineage_digest"))
        {
            bail!("Treasury publication evidence is incomplete");
        }
    }
    if TreasuryDailyRateFamily::ALL
        .into_iter()
        .any(|family| !families.contains(&family))
    {
        bail!("Treasury publication evidence omitted a required family");
    }
    Ok(())
}

fn treasury_family(value: &str) -> Option<TreasuryDailyRateFamily> {
    match value {
        "nominal_par_yield_curve" => Some(TreasuryDailyRateFamily::NominalParYieldCurve),
        "bill_rates" => Some(TreasuryDailyRateFamily::BillRates),
        "long_term_rates" => Some(TreasuryDailyRateFamily::LongTermRates),
        "real_par_yield_curve" => Some(TreasuryDailyRateFamily::RealParYieldCurve),
        "real_long_term_rates" => Some(TreasuryDailyRateFamily::RealLongTermRates),
        _ => None,
    }
}

fn treasury_dataset_year(family: TreasuryDailyRateFamily, dataset: &str) -> Option<u16> {
    let prefix = match family {
        TreasuryDailyRateFamily::NominalParYieldCurve => "treasury:daily-par-yield-curve:",
        TreasuryDailyRateFamily::BillRates => "treasury:daily-bill-rates:",
        TreasuryDailyRateFamily::LongTermRates => "treasury:daily-long-term-rates:",
        TreasuryDailyRateFamily::RealParYieldCurve => "treasury:daily-real-par-yield-curve:",
        TreasuryDailyRateFamily::RealLongTermRates => "treasury:daily-real-long-term-rates:",
    };
    dataset
        .strip_prefix(prefix)?
        .parse::<u16>()
        .ok()
        .filter(|year| (family.start_year()..=9999).contains(year))
}

fn treasury_source_object_matches(
    identity: &str,
    request_digest: [u8; 32],
    payload_digest: &str,
) -> bool {
    let request_digest = lower_hex(&request_digest);
    let mut fields = identity.split(':');
    fields.next() == Some("treasury-page")
        && fields.next() == Some("daily-rate")
        && fields.next() == Some("0")
        && fields.next() == Some(request_digest.as_str())
        && fields.next() == Some(payload_digest)
        && fields.next().is_none()
}

fn evidence_digest_hex(value: &Value) -> Option<String> {
    if value.pointer("/algorithm").and_then(Value::as_str) != Some("sha256") {
        return None;
    }
    let bytes = value.pointer("/bytes")?.as_array()?;
    if bytes.len() != 32 {
        return None;
    }
    let bytes = bytes
        .iter()
        .map(Value::as_u64)
        .map(|value| value.and_then(|value| u8::try_from(value).ok()))
        .collect::<Option<Vec<_>>>()?;
    if bytes.iter().all(|byte| *byte == 0) {
        return None;
    }
    Some(lower_hex(&bytes))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn valid_sha256(value: Option<&Value>) -> bool {
    value.and_then(Value::as_str).is_some_and(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
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
