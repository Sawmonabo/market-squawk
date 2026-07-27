//! Strict provider-report predicates used by exact-head closure and demonstration admission.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU16,
};

use anyhow::{Result, bail};
use market_squawk_adapter_bls::BlsSource;
use market_squawk_adapter_fred::FredSource;
use market_squawk_adapter_treasury::{
    TreasuryDailyRateFamily, TreasuryDailyRateQuery, TreasuryFiscalQuery,
};
use market_squawk_data::DatasetId;
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DataQuality, DigestAlgorithm, MacroObservation,
    PayloadReference, ResearchObservation, SourceIdentifier,
};
use market_squawk_sources::DataUseOperation;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

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
const DATA_USE_OPERATIONS: [DataUseOperation; 6] = [
    DataUseOperation::Retrieve,
    DataUseOperation::Display,
    DataUseOperation::Persist,
    DataUseOperation::ModelTraining,
    DataUseOperation::Export,
    DataUseOperation::Redistribute,
];
const MAXIMUM_TRAINING_REQUEST_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_TRAINING_PARENTS: usize = 64;
const BLS_UNEMPLOYMENT_SERIES: &str = "LNS14000000";
const BLS_PUBLIC_MAXIMUM_ACCEPTANCE_ROWS: u64 = 10 * 13;
const BLS_REGISTERED_MAXIMUM_ACCEPTANCE_ROWS: u64 = 20 * 13;
const SEC_SUBMISSIONS_FAMILY: &str = "sec_submissions_filings";
const SEC_COMPANY_FACTS_FAMILY: &str = "sec_company_facts";
const SEC_SUBMISSIONS_OPERATION: &str = "Fundamental.GetFilings";
const SEC_COMPANY_FACTS_OPERATION: &str = "Fundamental.GetFacts";
const FRED_SOURCE_ID: &str = "fred-fred-alfred.api-v1-v2";
const MAXIMUM_FRED_RELEASE_ROWS: usize = 1_024;
const MAXIMUM_FRED_RELEASE_ROW_BYTES: usize = 1024 * 1024;
const MAXIMUM_TREASURY_FISCAL_RELEASE_PAGES: usize = 1_023;
const MAXIMUM_TREASURY_FISCAL_RELEASE_ROWS: usize = 1_024;
const MAXIMUM_TREASURY_FISCAL_RELEASE_ROW_BYTES: usize = 1024 * 1024;
const TREASURY_FISCAL_SOURCE_ID: &str = "treasury-treasury.fiscal-data";

pub(super) fn validate_provider_evidence(payload: &Value) -> Result<()> {
    if payload.pointer("/schema_version").and_then(Value::as_u64) != Some(5)
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
    if selected.len() != REQUIRED_PROVIDER_SURFACES.len()
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
    let fred_surface = surfaces
        .iter()
        .find(|surface| {
            surface.pointer("/surface_id").and_then(Value::as_str) == Some("fred-alfred.api-v1-v2")
        })
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED surface evidence is absent"))?;
    validate_fred_rights_summary(payload, fred_surface)?;
    Ok(())
}

fn validate_fred_rights_summary(payload: &Value, surface: &Value) -> Result<()> {
    let summary = payload
        .pointer("/fred_alfred_rights")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED rights summary is absent"))?;
    let runtime_value = surface
        .pointer("/research_runtime")
        .filter(|runtime| !runtime.is_null())
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED research runtime is absent"))?;
    let runtime = runtime_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED research runtime is invalid"))?;
    validate_fred_runtime_authority(runtime_value)?;
    let expiry = runtime
        .get("rights_authorization_expires_at_unix_nanos")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED authority expiry is absent"))?;
    let collected_at = payload
        .pointer("/collected_at")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| value.timestamp_nanos_opt())
        .ok_or_else(|| anyhow::anyhow!("provider collection time is invalid"))?;
    if summary.get("required").and_then(Value::as_bool) != Some(true)
        || summary.get("selected").and_then(Value::as_bool) != Some(true)
        || summary.get("persistence_admitted").and_then(Value::as_bool) != Some(true)
        || summary
            .get("model_training_admitted")
            .and_then(Value::as_bool)
            != Some(true)
        || summary.get("admitted").and_then(Value::as_bool) != Some(true)
        || summary.get("parent_authorization_digest")
            != runtime.get("parent_rights_authorization_digest")
        || summary.get("authorization_digest") != runtime.get("rights_authorization_digest")
        || runtime.get("parent_rights_authorization_digest")
            != surface.pointer("/activation/rights_decision_digest")
        || runtime.get("session_id") != surface.pointer("/session/session_id")
        || runtime.get("session_id") != surface.pointer("/activation/session_id")
        || runtime.get("capability_revision") != surface.pointer("/activation/capability_revision")
        || runtime.get("capability_digest") != surface.pointer("/activation/capability_digest")
        || runtime.get("authority_effective_at_unix_nanos")
            != surface.pointer("/activation/authority_effective_at_unix_nanos")
        || summary
            .get("authorization_expires_at_unix_nanos")
            .and_then(Value::as_i64)
            != Some(expiry)
        || summary.get("exact_series") != runtime.get("rights_subjects")
        || expiry <= collected_at
    {
        bail!("FRED/ALFRED rights summary is not bound to current exact-series runtime authority");
    }
    Ok(())
}

fn validate_fred_runtime_authority(runtime: &Value) -> Result<()> {
    let parent = runtime
        .get("parent_rights_authorization_digest")
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED parent rights digest is absent"))?;
    let subordinate = runtime
        .get("rights_authorization_digest")
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED subordinate rights digest is absent"))?;
    let effective = runtime
        .get("authority_effective_at_unix_nanos")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED authority effective time is absent"))?;
    let expiry = runtime
        .get("rights_authorization_expires_at_unix_nanos")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED authority expiry is absent"))?;
    let subjects = runtime
        .get("rights_subjects")
        .and_then(Value::as_array)
        .filter(|subjects| subjects.len() == 1)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED exact-series authority is absent"))?;
    let subject = subjects
        .first()
        .and_then(Value::as_str)
        .filter(|value| SourceIdentifier::try_from(*value).is_ok())
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED exact-series authority is invalid"))?;
    let operations = string_set(
        runtime
            .get("rights_operations")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED operation authority is absent"))?,
        "FRED/ALFRED rights operations",
    )?;
    let allowed = BTreeSet::from(["display", "persist", "cache", "redistribute", "train"]);
    if runtime.get("source_id").and_then(Value::as_str) != Some(FRED_SOURCE_ID)
        || !nonzero_evidence_digest(parent)
        || !nonzero_evidence_digest(subordinate)
        || parent == subordinate
        || expiry <= effective
        || subject.is_empty()
        || operations
            .iter()
            .any(|operation| !allowed.contains(operation.as_str()))
        || ["display", "persist", "cache", "train"]
            .iter()
            .any(|required| !operations.contains(*required))
    {
        bail!("FRED/ALFRED runtime authority is not finite, exact-series, and operation scoped");
    }
    Ok(())
}

fn fred_runtime_series(runtime: &Value) -> Result<&str> {
    runtime
        .get("rights_subjects")
        .and_then(Value::as_array)
        .filter(|subjects| subjects.len() == 1)
        .and_then(|subjects| subjects.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED runtime exact series is absent"))
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
            if surface_id == "fred-alfred.api-v1-v2" {
                validate_fred_runtime_authority(runtime)?;
            }
            if matches!(surface_id, "bls.v1-unregistered" | "bls.v2-registered")
                && (surface
                    .pointer("/activation/data_use_admission/persist")
                    .and_then(Value::as_bool)
                    != Some(true)
                    || surface
                        .pointer("/activation/data_use_admission/model_training")
                        .and_then(Value::as_bool)
                        != Some(true))
            {
                bail!("BLS runtime lacks admitted persistence and model-training operations");
            }
            if matches!(
                surface_id,
                "treasury.daily-rates-xml" | "treasury.fiscal-data"
            ) && DATA_USE_OPERATIONS.iter().any(|operation| {
                surface
                    .pointer(&format!(
                        "/activation/data_use_admission/{}",
                        operation.evidence_name()
                    ))
                    .and_then(Value::as_bool)
                    != Some(true)
            }) {
                bail!("Treasury daily-rate runtime lacks admitted durable-use operations");
            }
            if surface_id == "treasury.daily-rates-xml" {
                validate_treasury_publications(runtime)?;
            } else if surface_id == "treasury.fiscal-data" {
                validate_treasury_fiscal_publication(runtime)?;
            } else if surface_id == "fred-alfred.api-v1-v2" {
                validate_fred_publications(runtime)?;
            } else if matches!(surface_id, "bls.v1-unregistered" | "bls.v2-registered") {
                validate_bls_publication(runtime, surface_id)?;
            } else if surface_id == "sec.edgar-public" {
                validate_sec_publications(runtime)?;
            } else if runtime
                .pointer("/publications")
                .and_then(Value::as_array)
                .is_none_or(|publications| !publications.is_empty())
                || !runtime
                    .pointer("/python_training")
                    .is_some_and(Value::is_null)
            {
                bail!("research runtime contains unexpected publication or training evidence");
            }
        }
        _ => bail!("provider evidence contains an unknown surface"),
    }
    Ok(())
}

fn validate_treasury_fiscal_publication(runtime: &Value) -> Result<()> {
    if !runtime
        .pointer("/python_training")
        .is_some_and(Value::is_null)
    {
        bail!("Treasury Fiscal Data runtime contains unexpected Python training evidence");
    }
    let publications = runtime
        .pointer("/publications")
        .and_then(Value::as_array)
        .filter(|publications| publications.len() == 1)
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data publication evidence is absent"))?;
    let publication = publications
        .first()
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data publication evidence is absent"))?;
    let fiscal = publication
        .get("treasury_fiscal")
        .and_then(Value::as_object)
        .filter(|fiscal| {
            fiscal.len() == 7
                && [
                    "first_record_date",
                    "last_record_date",
                    "page_size",
                    "query_digest",
                    "provider_row_count",
                    "pages",
                    "observation_query",
                ]
                .into_iter()
                .all(|field| fiscal.contains_key(field))
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Treasury Fiscal Data publication evidence contains unknown or missing fields"
            )
        })?;
    let first_record_date: CalendarDate = serde_json::from_value(
        fiscal
            .get("first_record_date")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data first date is absent"))?,
    )
    .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data first date is invalid"))?;
    let last_record_date: CalendarDate = serde_json::from_value(
        fiscal
            .get("last_record_date")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data final date is absent"))?,
    )
    .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data final date is invalid"))?;
    let page_size = fiscal
        .get("page_size")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .and_then(NonZeroU16::new)
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data page size is invalid"))?;
    let query = TreasuryFiscalQuery::average_interest_rates_v2(
        first_record_date,
        last_record_date,
        page_size,
    )
    .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data query is invalid"))?;
    let provider_dataset = query
        .dataset()
        .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data provider dataset is invalid"))?;
    let analytical_dataset = query
        .analytical_dataset()
        .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data analytical dataset is invalid"))?;
    let provider_row_count = fiscal
        .get("provider_row_count")
        .and_then(Value::as_u64)
        .filter(|rows| *rows > 0)
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data provider row count is invalid"))?;
    if fiscal.get("query_digest").and_then(Value::as_str)
        != Some(lower_hex(&query.query_digest()).as_str())
        || publication.get("family").and_then(Value::as_str) != Some("average_interest_rates_v2")
        || publication.get("provider_dataset").and_then(Value::as_str)
            != Some(provider_dataset.as_str())
        || publication
            .get("analytical_dataset_id")
            .and_then(Value::as_str)
            != Some(analytical_dataset.as_str())
        || DatasetId::try_from(analytical_dataset.as_str()).is_err()
        || publication
            .get("temporal_semantics")
            .and_then(Value::as_str)
            != Some("treasury_fiscal_effective_observations")
        || publication.get("row_count").and_then(Value::as_u64) != Some(provider_row_count)
        || publication
            .get("observation_query_row_count")
            .and_then(Value::as_u64)
            != Some(provider_row_count)
        || publication.get("sec").is_none_or(|value| !value.is_null())
        || publication.get("fred").is_none_or(|value| !value.is_null())
    {
        bail!("Treasury Fiscal Data publication lost its exact query or manifest authority");
    }

    let page_rows =
        validate_treasury_fiscal_pages(publication, fiscal, &query, provider_row_count)?;
    let observed_series = validate_treasury_fiscal_query_evidence(
        fiscal.get("observation_query"),
        &query,
        &page_rows,
        provider_row_count,
    )?;
    let declared_series = string_set(
        publication
            .get("series_ids")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data series evidence is absent"))?,
        "Treasury Fiscal Data series",
    )?;
    if declared_series.is_empty() || declared_series != observed_series {
        bail!("Treasury Fiscal Data series evidence does not match the canonical row set");
    }
    validate_research_publication(publication, "treasury.fiscal-data", false)
}

struct TreasuryFiscalPageAuthority {
    request_digest: [u8; 32],
    returned_rows: u64,
}

fn validate_treasury_fiscal_pages(
    publication: &Value,
    fiscal: &serde_json::Map<String, Value>,
    query: &TreasuryFiscalQuery,
    provider_row_count: u64,
) -> Result<BTreeMap<[u8; 32], TreasuryFiscalPageAuthority>> {
    let pages = fiscal
        .get("pages")
        .and_then(Value::as_array)
        .filter(|pages| !pages.is_empty() && pages.len() <= MAXIMUM_TREASURY_FISCAL_RELEASE_PAGES)
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data page-chain evidence is absent"))?;
    if publication.get("object_count").and_then(Value::as_u64) != u64::try_from(pages.len()).ok() {
        bail!("Treasury Fiscal Data object count does not match its complete page chain");
    }
    let mut source_objects = BTreeSet::new();
    let mut page_rows = BTreeMap::new();
    let mut accounted_rows = 0_u64;
    let mut final_object = None;
    let mut final_payload = None;
    for (index, page) in pages.iter().enumerate() {
        let page = page
            .as_object()
            .filter(|page| {
                page.len() == 5
                    && [
                        "source_object_id",
                        "source_payload_digest",
                        "page_number",
                        "request_digest",
                        "returned_rows",
                    ]
                    .into_iter()
                    .all(|field| page.contains_key(field))
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Treasury Fiscal Data page evidence contains unknown or missing fields"
                )
            })?;
        let page_number = index
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data page number overflow"))?;
        let expected_request = query
            .page(page_number)
            .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data page request is invalid"))?;
        let request_digest_bytes = expected_request.request_digest();
        let request_digest = lower_hex(&request_digest_bytes);
        let source_object = page
            .get("source_object_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data source object is invalid"))?;
        let payload_digest = page
            .get("source_payload_digest")
            .and_then(evidence_digest_bytes)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data page payload is invalid"))?;
        let returned_rows = page
            .get("returned_rows")
            .and_then(Value::as_u64)
            .filter(|rows| *rows > 0)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data page row count is invalid"))?;
        if page.get("page_number").and_then(Value::as_u64) != u64::try_from(page_number).ok()
            || page.get("request_digest").and_then(Value::as_str) != Some(request_digest.as_str())
            || !treasury_fiscal_source_object_matches(
                source_object,
                page_number,
                &request_digest,
                &lower_hex(&payload_digest),
            )
            || !source_objects.insert(source_object)
            || page_rows
                .insert(
                    payload_digest,
                    TreasuryFiscalPageAuthority {
                        request_digest: request_digest_bytes,
                        returned_rows,
                    },
                )
                .is_some()
        {
            bail!("Treasury Fiscal Data page chain is duplicated or inconsistent");
        }
        accounted_rows = accounted_rows
            .checked_add(returned_rows)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data row count overflow"))?;
        final_object = Some(source_object);
        final_payload = Some(payload_digest);
    }
    if accounted_rows != provider_row_count
        || publication.get("source_object_id").and_then(Value::as_str) != final_object
        || publication
            .get("source_payload_digest")
            .and_then(evidence_digest_bytes)
            != final_payload
    {
        bail!("Treasury Fiscal Data final publication does not bind its complete page chain");
    }
    Ok(page_rows)
}

fn validate_treasury_fiscal_query_evidence(
    query_evidence: Option<&Value>,
    query: &TreasuryFiscalQuery,
    page_rows: &BTreeMap<[u8; 32], TreasuryFiscalPageAuthority>,
    provider_row_count: u64,
) -> Result<BTreeSet<String>> {
    let query_evidence = query_evidence
        .and_then(Value::as_object)
        .filter(|query| {
            query.len() == 3
                && ["row_count", "content_sha256", "rows"]
                    .into_iter()
                    .all(|field| query.contains_key(field))
        })
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data inline query evidence is absent"))?;
    let rows = query_evidence
        .get("rows")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty() && rows.len() <= MAXIMUM_TREASURY_FISCAL_RELEASE_ROWS)
        .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data inline query rows are invalid"))?;
    let row_count = u64::try_from(rows.len())
        .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data query row count overflow"))?;
    let encoded = serde_json::to_vec(rows)
        .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data rows are invalid"))?;
    let digest: [u8; 32] = Sha256::digest(&encoded).into();
    if encoded.len() > MAXIMUM_TREASURY_FISCAL_RELEASE_ROW_BYTES
        || row_count != provider_row_count
        || query_evidence.get("row_count").and_then(Value::as_u64) != Some(row_count)
        || query_evidence.get("content_sha256").and_then(Value::as_str)
            != Some(lower_hex(&digest).as_str())
    {
        bail!("Treasury Fiscal Data query count, size, or content digest is invalid");
    }
    validate_treasury_fiscal_rows(rows, query, page_rows)
}

fn validate_treasury_fiscal_rows(
    rows: &[Value],
    query: &TreasuryFiscalQuery,
    expected_page_rows: &BTreeMap<[u8; 32], TreasuryFiscalPageAuthority>,
) -> Result<BTreeSet<String>> {
    let mut observed_page_rows = BTreeMap::<[u8; 32], u64>::new();
    let mut identities = BTreeSet::new();
    let mut series = BTreeSet::new();
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data query row is invalid"))?;
        let payload =
            required_lower_hex_bytes(row.get("payload_json"), "Treasury Fiscal Data payload")?;
        let declared_payload_digest = required_lower_hex_bytes(
            row.get("payload_sha256"),
            "Treasury Fiscal Data payload digest",
        )?;
        let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        if declared_payload_digest.as_slice() != payload_digest {
            bail!("Treasury Fiscal Data canonical payload digest is invalid");
        }
        let observation: ResearchObservation = serde_json::from_slice(&payload)
            .map_err(|_| anyhow::anyhow!("Treasury Fiscal Data canonical payload is invalid"))?;
        let ResearchObservation::Macro(observation) = observation else {
            bail!("Treasury Fiscal Data query returned a non-macro observation");
        };
        let request_digest =
            required_lower_hex_bytes(row.get("request_sha256"), "Treasury Fiscal Data request")?;
        let lineage = required_lower_hex_bytes(
            row.get("extraction_lineage_json"),
            "Treasury Fiscal Data extraction lineage",
        )?;
        let context = observation.context();
        let provenance = context.provenance();
        let effective = context
            .time()
            .effective()
            .calendar_date_value()
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data effective date is absent"))?;
        let source_identifier = provenance.source_identifier().as_str();
        let expected_prefix = format!("treasury-fiscal-rate:{effective}:");
        let observed_value = observation
            .value()
            .observed_value()
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data value is absent"))?;
        if observation.value().missing_value().is_some() {
            bail!("Treasury Fiscal Data value cannot be both observed and missing");
        }
        let received_at = row_timestamp_nanos(row.get("received_at"));
        let available_at = row_timestamp_nanos(row.get("available_at"));
        let ingested_at = row_timestamp_nanos(row.get("ingested_at"));
        let page_digest = match provenance.payload_reference() {
            PayloadReference::ContentHash(hash) if hash.algorithm() == DigestAlgorithm::Sha256 => {
                hash.digest()
            }
            _ => bail!("Treasury Fiscal Data row omitted exact provider-page evidence"),
        };
        let expected_page = expected_page_rows.get(&page_digest).ok_or_else(|| {
            anyhow::anyhow!("Treasury Fiscal Data row references an unadmitted provider page")
        })?;
        let observed = observed_page_rows.entry(page_digest).or_default();
        *observed = observed
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Treasury Fiscal Data page row count overflow"))?;
        if request_digest.as_slice() != expected_page.request_digest.as_slice()
            || serde_json::from_slice::<Value>(&lineage)
                .ok()
                .is_none_or(|value| value.is_null())
            || row.keys().any(|field| !fred_row_field_allowed(field))
            || effective < query.first_record_date()
            || effective > query.last_record_date()
            || !source_identifier.starts_with(&expected_prefix)
            || !treasury_fiscal_revision_matches(source_identifier, effective)
            || provenance.source_id().as_str() != TREASURY_FISCAL_SOURCE_ID
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_timestamp().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.ingested_at() < provenance.received_at()
            || !matches!(
                provenance.availability(),
                AvailabilityEvidence::LocalFirstObserved { observed_at }
                    if *observed_at == provenance.received_at()
            )
            || context.time().published().is_some()
            || context.time().superseded().is_some()
            || context.time().revision().get() != 1
            || !treasury_fiscal_series_valid(observation.series().as_str())
            || observation.unit().as_str() != "percent"
            || row.get("schema_version").and_then(Value::as_u64) != Some(3)
            || received_at != Some(provenance.received_at().unix_nanos())
            || available_at != Some(provenance.received_at().unix_nanos())
            || ingested_at != Some(provenance.ingested_at().unix_nanos())
            || row.get("observation_kind").and_then(Value::as_str) != Some("macro")
            || row.get("source_id").and_then(Value::as_str) != Some(TREASURY_FISCAL_SOURCE_ID)
            || row.get("source_identifier").and_then(Value::as_str) != Some(source_identifier)
            || row.get("received_at") != row.get("available_at")
            || row.get("availability_kind").and_then(Value::as_str) != Some("local_first_observed")
            || row.get("effective_precision").and_then(Value::as_str) != Some("calendar_date")
            || row.get("effective_date").and_then(Value::as_str)
                != Some(effective.to_string().as_str())
            || row.get("effective_at").is_some()
            || row.get("effective_period_scheme").is_some()
            || row.get("effective_period_year").is_some()
            || row.get("effective_period_ordinal").is_some()
            || row.get("effective_period_code").is_some()
            || row.get("published_at").is_some()
            || row.get("published_date").is_some()
            || row.get("published_period_scheme").is_some()
            || row.get("published_period_year").is_some()
            || row.get("published_period_ordinal").is_some()
            || row.get("published_period_code").is_some()
            || row.get("superseded_at").is_some()
            || row.get("superseded_date").is_some()
            || row.get("superseded_period_scheme").is_some()
            || row.get("superseded_period_year").is_some()
            || row.get("superseded_period_ordinal").is_some()
            || row.get("superseded_period_code").is_some()
            || row.get("revision").and_then(Value::as_u64) != Some(1)
            || row.get("quality").and_then(Value::as_str) != Some("official_delayed")
            || row.get("value_state").and_then(Value::as_str) != Some("observed")
            || row.get("value_mantissa").and_then(json_i128) != Some(observed_value.mantissa())
            || row.get("value_scale").and_then(Value::as_u64)
                != Some(u64::from(observed_value.scale()))
            || row.get("missing_marker").is_some()
            || row.get("missing_reason").is_some()
            || row.get("unit").and_then(Value::as_str) != Some("percent")
            || row.get("currency").is_some()
            || row.get("instrument_id").is_some()
            || row.get("venue_id").is_some()
            || row.get("source_timestamp").is_some()
            || row.get("availability_reported_or_inferred_at").is_some()
            || row.get("availability_evidence").is_some()
            || row.get("availability_method").is_some()
            || row.get("published_precision").is_some()
            || row.get("superseded_precision").is_some()
            || !identities.insert((source_identifier.to_owned(), payload_digest))
        {
            bail!(
                "Treasury Fiscal Data row lost exact source, time, quality, or payload authority"
            );
        }
        series.insert(observation.series().as_str().to_owned());
    }
    if observed_page_rows.len() != expected_page_rows.len()
        || expected_page_rows.iter().any(|(digest, authority)| {
            observed_page_rows.get(digest) != Some(&authority.returned_rows)
        })
        || series.is_empty()
    {
        bail!("Treasury Fiscal Data rows do not exactly cover every admitted provider page");
    }
    Ok(series)
}

fn treasury_fiscal_revision_matches(identity: &str, effective: CalendarDate) -> bool {
    let mut fields = identity.split(':');
    fields.next() == Some("treasury-fiscal-rate")
        && fields.next() == Some(effective.to_string().as_str())
        && fields
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|line| line > 0)
        && fields.next().is_some_and(valid_nonzero_sha256_text)
        && fields.next().is_none()
}

fn treasury_fiscal_series_valid(series: &str) -> bool {
    let mut fields = series.split(':');
    fields.next() == Some("treasury")
        && fields.next() == Some("average-interest-rate")
        && fields.next() == Some("v2")
        && fields.next().is_some_and(|value| !value.is_empty())
        && fields.next().is_some_and(|value| !value.is_empty())
        && fields.next().is_none()
}

fn validate_sec_publications(runtime: &Value) -> Result<()> {
    if !runtime
        .pointer("/python_training")
        .is_some_and(Value::is_null)
    {
        bail!("SEC runtime contains unexpected Python training evidence");
    }
    let publications = runtime
        .pointer("/publications")
        .and_then(Value::as_array)
        .filter(|publications| publications.len() == 2)
        .ok_or_else(|| anyhow::anyhow!("SEC filings and Company Facts evidence is absent"))?;
    let mut families = BTreeSet::new();
    let mut provider_datasets = BTreeSet::new();
    let mut analytical_datasets = BTreeSet::new();
    let mut source_objects = BTreeSet::new();
    let mut source_payloads = BTreeSet::new();
    let mut manifest_hashes = BTreeSet::new();
    let mut common_cik = None;
    let mut common_instrument = None;
    for publication in publications {
        let family = publication
            .get("family")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("SEC publication family is absent"))?;
        let sec = publication
            .get("sec")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("SEC row-provenance evidence is absent"))?;
        let cik = sec
            .get("cik")
            .and_then(Value::as_str)
            .filter(|value| valid_sec_cik(value))
            .ok_or_else(|| anyhow::anyhow!("SEC publication CIK is invalid"))?;
        if common_cik
            .replace(cik)
            .is_some_and(|expected| expected != cik)
        {
            bail!("SEC publications do not use one exact CIK");
        }
        let instrument = sec
            .get("instrument_id")
            .and_then(Value::as_str)
            .filter(|value| {
                uuid::Uuid::parse_str(value).is_ok_and(|instrument| !instrument.is_nil())
            })
            .ok_or_else(|| anyhow::anyhow!("SEC publication instrument identity is invalid"))?;
        if common_instrument
            .replace(instrument)
            .is_some_and(|expected| expected != instrument)
        {
            bail!("SEC publications do not bind one stable instrument identity");
        }
        let (expected_dataset, expected_object, expected_operation, expected_kind) = match family {
            SEC_SUBMISSIONS_FAMILY => (
                format!("sec.submissions.cik.{cik}"),
                format!("sec.submissions.composite.CIK{cik}"),
                SEC_SUBMISSIONS_OPERATION,
                "filing",
            ),
            SEC_COMPANY_FACTS_FAMILY => (
                format!("sec.company-facts.cik.{cik}"),
                format!("https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"),
                SEC_COMPANY_FACTS_OPERATION,
                "fundamental",
            ),
            _ => bail!("SEC publication family is invalid"),
        };
        let provider_dataset = publication
            .get("provider_dataset")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("SEC provider dataset is absent"))?;
        let analytical_dataset = publication
            .get("analytical_dataset_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("SEC analytical dataset is absent"))?;
        let source_object = publication
            .get("source_object_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("SEC source object is absent"))?;
        let source_payload = publication
            .get("source_payload_digest")
            .and_then(evidence_digest_hex)
            .ok_or_else(|| anyhow::anyhow!("SEC source payload digest is absent"))?;
        let manifest_hash = publication
            .get("manifest_content_hash")
            .and_then(Value::as_str)
            .filter(|value| valid_nonzero_sha256_text(value))
            .ok_or_else(|| anyhow::anyhow!("SEC manifest content hash is invalid"))?;
        let row_count = publication
            .get("row_count")
            .and_then(Value::as_u64)
            .filter(|rows| *rows > 0)
            .ok_or_else(|| anyhow::anyhow!("SEC publication row count is invalid"))?;
        if provider_dataset != expected_dataset
            || analytical_dataset != expected_dataset
            || DatasetId::try_from(analytical_dataset).is_err()
            || source_object != expected_object
            || sec.get("query_operation").and_then(Value::as_str) != Some(expected_operation)
            || sec.get("observation_kind").and_then(Value::as_str) != Some(expected_kind)
            || sec.get("quality").and_then(Value::as_str) != Some("official_delayed")
            || sec.get("provenance_verified_rows").and_then(Value::as_u64) != Some(row_count)
            || publication
                .get("observation_query_row_count")
                .and_then(Value::as_u64)
                != Some(row_count)
            || publication
                .get("temporal_semantics")
                .and_then(Value::as_str)
                != Some("locally_observed_sec_disclosure")
            || publication
                .get("series_ids")
                .and_then(Value::as_array)
                .is_none_or(|series| !series.is_empty())
        {
            bail!("SEC publication lost its exact CIK, query, quality, time, or row authority");
        }
        if !families.insert(family)
            || !provider_datasets.insert(provider_dataset)
            || !analytical_datasets.insert(analytical_dataset)
            || !source_objects.insert(source_object)
            || !source_payloads.insert(source_payload)
            || !manifest_hashes.insert(manifest_hash)
        {
            bail!("SEC filings and Company Facts publications are not distinct");
        }
        validate_research_publication(publication, "sec.edgar-public", false)?;
    }
    if families.len() != 2
        || provider_datasets.len() != 2
        || analytical_datasets.len() != 2
        || source_objects.len() != 2
        || source_payloads.len() != 2
        || manifest_hashes.len() != 2
    {
        bail!("SEC evidence does not contain both distinct required publications");
    }
    Ok(())
}

fn valid_sec_cik(value: &str) -> bool {
    value.len() == 10
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
}

fn validate_treasury_publications(runtime: &Value) -> Result<()> {
    if !runtime
        .pointer("/python_training")
        .is_some_and(Value::is_null)
    {
        bail!("Treasury runtime contains unexpected Python training evidence");
    }
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
            .ok_or_else(|| anyhow::anyhow!("Treasury analytical dataset identity is invalid"))?;
        if query.analytical_dataset().as_str() != analytical_dataset
            || DatasetId::try_from(analytical_dataset).is_err()
        {
            bail!("Treasury analytical dataset is not bound to its provider selector");
        }
        if source_object.is_empty()
            || !families.insert(family)
            || !provider_datasets.insert(provider_dataset)
            || !analytical_datasets.insert(analytical_dataset)
            || publication.get("object_count").and_then(Value::as_u64) != Some(1)
            || publication
                .get("temporal_semantics")
                .and_then(Value::as_str)
                != Some("effective_observations")
            || publication
                .get("series_ids")
                .and_then(Value::as_array)
                .is_none_or(|series| !series.is_empty())
        {
            bail!("Treasury publication evidence is incomplete");
        }
        validate_research_publication(publication, "treasury.daily-rates-xml", false)?;
    }
    if TreasuryDailyRateFamily::ALL
        .into_iter()
        .any(|family| !families.contains(&family))
    {
        bail!("Treasury publication evidence omitted a required family");
    }
    Ok(())
}

fn validate_fred_publications(runtime: &Value) -> Result<()> {
    validate_fred_runtime_authority(runtime)?;
    let authorized_series = fred_runtime_series(runtime)?;
    let publications = runtime
        .pointer("/publications")
        .and_then(Value::as_array)
        .filter(|publications| publications.len() == 1)
        .ok_or_else(|| anyhow::anyhow!("one complete FRED/ALFRED publication is required"))?;
    let publication = publications
        .first()
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED publication evidence is absent"))?;
    let fred = publication
        .get("fred")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED complete-publication evidence is absent"))?;
    if fred.len() != 7
        || ![
            "series_id",
            "realtime_start",
            "realtime_end",
            "provider_row_count",
            "pages",
            "observation_query",
            "vintage_query",
        ]
        .into_iter()
        .all(|field| fred.contains_key(field))
    {
        bail!("FRED/ALFRED complete-publication evidence contains unknown or missing fields");
    }
    let provider_dataset = publication
        .get("provider_dataset")
        .and_then(Value::as_str)
        .and_then(|value| SourceIdentifier::try_from(value).ok())
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED provider dataset is invalid"))?;
    let series = FredSource::rights_subject_identifier(&provider_dataset)
        .map_err(|_| anyhow::anyhow!("FRED/ALFRED provider dataset has no exact series"))?;
    let (realtime_start, realtime_end) =
        FredSource::dataset_realtime_interval(&provider_dataset)
            .map_err(|_| anyhow::anyhow!("FRED/ALFRED provider real-time interval is invalid"))?;
    let realtime_start = realtime_start.to_string();
    let realtime_end = realtime_end.to_string();
    let analytical_dataset = FredSource::analytical_dataset_identifier(&provider_dataset)
        .map_err(|_| anyhow::anyhow!("FRED/ALFRED provider dataset is invalid"))?;
    let provider_row_count = fred
        .get("provider_row_count")
        .and_then(Value::as_u64)
        .filter(|count| *count > 0)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED provider row count is invalid"))?;
    let series_ids = publication
        .get("series_ids")
        .and_then(Value::as_array)
        .filter(|values| {
            values.len() == 1 && values.first().and_then(Value::as_str) == Some(series.as_str())
        })
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED publication series is invalid"))?;
    if publication.get("family").and_then(Value::as_str) != Some("fred_alfred_vintages")
        || publication
            .get("temporal_semantics")
            .and_then(Value::as_str)
            != Some("provider_reported_vintages")
        || publication.get("sec").is_none_or(|value| !value.is_null())
        || series.as_str() != authorized_series
        || fred.get("series_id").and_then(Value::as_str) != Some(series.as_str())
        || fred.get("realtime_start").and_then(Value::as_str) != Some(realtime_start.as_str())
        || fred.get("realtime_end").and_then(Value::as_str) != Some(realtime_end.as_str())
        || publication
            .get("analytical_dataset_id")
            .and_then(Value::as_str)
            != Some(analytical_dataset.as_str())
        || DatasetId::try_from(analytical_dataset.as_str()).is_err()
        || publication.get("row_count").and_then(Value::as_u64) != Some(provider_row_count)
        || publication
            .get("observation_query_row_count")
            .and_then(Value::as_u64)
            != Some(provider_row_count)
        || publication
            .get("vintage_query_row_count")
            .and_then(Value::as_u64)
            != Some(provider_row_count)
        || series_ids.len() != 1
    {
        bail!("FRED/ALFRED publication lost its exact series, interval, or complete row authority");
    }

    let page_rows = validate_fred_pages(publication, fred, provider_row_count)?;
    let observation_rows = validate_fred_query_evidence(
        fred.get("observation_query"),
        &provider_dataset,
        &series,
        &page_rows,
        provider_row_count,
    )?;
    let vintage_rows = validate_fred_query_evidence(
        fred.get("vintage_query"),
        &provider_dataset,
        &series,
        &page_rows,
        provider_row_count,
    )?;
    if observation_rows != vintage_rows {
        bail!("FRED/ALFRED observation and vintage evidence are not the same exact row set");
    }
    validate_research_publication(publication, "fred-alfred.api-v1-v2", true)?;
    validate_python_training(
        runtime,
        publications,
        "fred-alfred.api-v1-v2",
        "FRED/ALFRED",
    )
}

fn validate_fred_pages(
    publication: &Value,
    fred: &serde_json::Map<String, Value>,
    provider_row_count: u64,
) -> Result<BTreeMap<[u8; 32], u64>> {
    let pages = fred
        .get("pages")
        .and_then(Value::as_array)
        .filter(|pages| !pages.is_empty() && pages.len() <= MAXIMUM_FRED_RELEASE_ROWS)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED page-chain evidence is absent"))?;
    if publication.get("object_count").and_then(Value::as_u64) != u64::try_from(pages.len()).ok() {
        bail!("FRED/ALFRED publication object count does not match its complete page chain");
    }
    let mut expected_offset = 0_usize;
    let mut metadata_digest = None;
    let mut source_objects = BTreeSet::new();
    let mut page_rows = BTreeMap::new();
    let mut final_object = None;
    let mut final_payload = None;
    for (index, page) in pages.iter().enumerate() {
        let page = page
            .as_object()
            .filter(|page| {
                page.len() == 7
                    && [
                        "source_object_id",
                        "source_payload_digest",
                        "offset",
                        "limit",
                        "returned_rows",
                        "provider_row_count",
                        "terminal",
                    ]
                    .into_iter()
                    .all(|field| page.contains_key(field))
            })
            .ok_or_else(|| {
                anyhow::anyhow!("FRED/ALFRED page evidence contains unknown or missing fields")
            })?;
        let source_object = page
            .get("source_object_id")
            .and_then(Value::as_str)
            .and_then(|value| SourceIdentifier::try_from(value).ok())
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED page identity is invalid"))?;
        let identity = FredSource::page_object_identity(&source_object)
            .map_err(|_| anyhow::anyhow!("FRED/ALFRED page identity is invalid"))?;
        let payload_digest = evidence_digest_bytes(
            page.get("source_payload_digest")
                .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED page payload digest is absent"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED page payload digest is invalid"))?;
        let terminal = index + 1 == pages.len();
        if identity.offset() != expected_offset
            || identity.page_digest() != payload_digest
            || page.get("offset").and_then(Value::as_u64) != u64::try_from(identity.offset()).ok()
            || page.get("limit").and_then(Value::as_u64) != u64::try_from(identity.limit()).ok()
            || page.get("returned_rows").and_then(Value::as_u64)
                != u64::try_from(identity.returned()).ok()
            || page.get("provider_row_count").and_then(Value::as_u64)
                != u64::try_from(identity.total()).ok()
            || page.get("terminal").and_then(Value::as_bool) != Some(terminal)
            || identity.terminal() != terminal
            || u64::try_from(identity.total()).ok() != Some(provider_row_count)
            || !source_objects.insert(source_object.as_str().to_owned())
            || page_rows
                .insert(
                    payload_digest,
                    u64::try_from(identity.returned())
                        .map_err(|_| anyhow::anyhow!("FRED/ALFRED page row count overflow"))?,
                )
                .is_some()
        {
            bail!("FRED/ALFRED page chain is incomplete, duplicated, or inconsistent");
        }
        if metadata_digest
            .replace(identity.metadata_digest())
            .is_some_and(|expected| expected != identity.metadata_digest())
            || identity.metadata_digest() == [0; 32]
        {
            bail!("FRED/ALFRED page chain does not share one exact metadata response");
        }
        expected_offset = expected_offset
            .checked_add(identity.returned())
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED page offset overflow"))?;
        final_object = Some(source_object.as_str().to_owned());
        final_payload = Some(payload_digest);
    }
    if u64::try_from(expected_offset).ok() != Some(provider_row_count)
        || publication.get("source_object_id").and_then(Value::as_str) != final_object.as_deref()
        || publication
            .get("source_payload_digest")
            .and_then(evidence_digest_bytes)
            != final_payload
    {
        bail!("FRED/ALFRED final publication does not bind the terminal complete page chain");
    }
    Ok(page_rows)
}

fn validate_fred_query_evidence<'a>(
    query: Option<&'a Value>,
    provider_dataset: &SourceIdentifier,
    series: &SourceIdentifier,
    page_rows: &BTreeMap<[u8; 32], u64>,
    provider_row_count: u64,
) -> Result<&'a [Value]> {
    let query = query
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED inline query evidence is absent"))?;
    if query.len() != 3
        || !query.contains_key("row_count")
        || !query.contains_key("content_sha256")
        || !query.contains_key("rows")
    {
        bail!("FRED/ALFRED query evidence has an unknown or incomplete representation");
    }
    let rows = query
        .get("rows")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty() && rows.len() <= MAXIMUM_FRED_RELEASE_ROWS)
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED inline query rows are invalid"))?;
    let row_count = u64::try_from(rows.len())
        .map_err(|_| anyhow::anyhow!("FRED/ALFRED query row count overflow"))?;
    let encoded =
        serde_json::to_vec(rows).map_err(|_| anyhow::anyhow!("FRED/ALFRED rows are invalid"))?;
    let digest: [u8; 32] = Sha256::digest(&encoded).into();
    if encoded.len() > MAXIMUM_FRED_RELEASE_ROW_BYTES
        || row_count != provider_row_count
        || query.get("row_count").and_then(Value::as_u64) != Some(row_count)
        || query.get("content_sha256").and_then(Value::as_str) != Some(lower_hex(&digest).as_str())
    {
        bail!("FRED/ALFRED query evidence count, size, or content digest is invalid");
    }
    validate_fred_rows(rows, provider_dataset, series, page_rows)?;
    Ok(rows)
}

fn validate_fred_rows(
    rows: &[Value],
    provider_dataset: &SourceIdentifier,
    series: &SourceIdentifier,
    expected_page_rows: &BTreeMap<[u8; 32], u64>,
) -> Result<()> {
    const DAYS_FROM_YEAR_ONE_TO_UNIX_EPOCH: i32 = 719_163;

    let namespace = provider_dataset
        .as_str()
        .split(':')
        .next()
        .filter(|value| matches!(*value, "fred" | "alfred"))
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED dataset namespace is invalid"))?;
    let mut observed_page_rows = BTreeMap::<[u8; 32], u64>::new();
    let mut identities = BTreeSet::new();
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED query row is invalid"))?;
        let payload =
            required_lower_hex_bytes(row.get("payload_json"), "FRED/ALFRED canonical payload")?;
        let declared_payload_digest = required_lower_hex_bytes(
            row.get("payload_sha256"),
            "FRED/ALFRED canonical payload digest",
        )?;
        let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        if declared_payload_digest.as_slice() != payload_digest {
            bail!("FRED/ALFRED canonical payload digest is invalid");
        }
        let observation: ResearchObservation = serde_json::from_slice(&payload)
            .map_err(|_| anyhow::anyhow!("FRED/ALFRED canonical payload is invalid"))?;
        let ResearchObservation::Macro(observation) = observation else {
            bail!("FRED/ALFRED query returned a non-macro observation");
        };
        validate_fred_row_projection(row, &observation, payload_digest)?;
        let context = observation.context();
        let provenance = context.provenance();
        let effective = context
            .time()
            .effective()
            .calendar_date_value()
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED effective date precision was lost"))?;
        let published = context
            .time()
            .published()
            .and_then(|value| value.calendar_date_value())
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED vintage date precision was lost"))?;
        if context
            .time()
            .superseded()
            .is_some_and(|value| value.calendar_date_value().is_none())
        {
            bail!("FRED/ALFRED supersession precision is invalid");
        }
        let revision = published
            .days_since_unix_epoch()
            .checked_add(DAYS_FROM_YEAR_ONE_TO_UNIX_EPOCH)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED revision is invalid"))?;
        let source_identifier = format!("{namespace}:{series}:{effective}:{published}");
        let page_digest = match provenance.payload_reference() {
            PayloadReference::ContentHash(hash) if hash.algorithm() == DigestAlgorithm::Sha256 => {
                hash.digest()
            }
            _ => bail!("FRED/ALFRED row omitted exact provider-page evidence"),
        };
        if !expected_page_rows.contains_key(&page_digest) {
            bail!("FRED/ALFRED row references a payload outside the admitted page chain");
        }
        let observed = observed_page_rows.entry(page_digest).or_default();
        *observed = observed
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED page row count overflow"))?;
        if observation.series() != series
            || provenance.source_id().as_str() != FRED_SOURCE_ID
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_identifier().as_str() != source_identifier
            || provenance.source_timestamp().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.ingested_at() < provenance.received_at()
            || !matches!(
                provenance.availability(),
                AvailabilityEvidence::LocalFirstObserved { observed_at }
                    if *observed_at == provenance.received_at()
            )
            || context.time().revision().get() != revision
            || row.get("observation_kind").and_then(Value::as_str) != Some("macro")
            || row.get("source_id").and_then(Value::as_str) != Some(FRED_SOURCE_ID)
            || row.get("source_identifier").and_then(Value::as_str)
                != Some(source_identifier.as_str())
            || row
                .get("instrument_id")
                .is_some_and(|value| !value.is_null())
            || row.get("venue_id").is_some_and(|value| !value.is_null())
            || row
                .get("source_timestamp")
                .is_some_and(|value| !value.is_null())
            || row.get("received_at") != row.get("available_at")
            || row.get("availability_kind").and_then(Value::as_str) != Some("local_first_observed")
            || row.get("effective_precision").and_then(Value::as_str) != Some("calendar_date")
            || row.get("published_precision").and_then(Value::as_str) != Some("calendar_date")
            || row.get("revision").and_then(Value::as_u64) != Some(u64::from(revision))
            || row.get("quality").and_then(Value::as_str) != Some("official_delayed")
            || row.get("unit").and_then(Value::as_str) != Some(observation.unit().as_str())
            || !identities.insert((source_identifier, payload_digest))
        {
            bail!("FRED/ALFRED row lost exact series, time, quality, or payload provenance");
        }
    }
    if &observed_page_rows != expected_page_rows {
        bail!("FRED/ALFRED query rows do not exactly cover every admitted provider page");
    }
    Ok(())
}

fn validate_fred_row_projection(
    row: &serde_json::Map<String, Value>,
    observation: &MacroObservation,
    payload_digest: [u8; 32],
) -> Result<()> {
    if row.keys().any(|field| !fred_row_field_allowed(field)) {
        bail!("FRED/ALFRED query row contains a field outside the canonical research schema");
    }
    let request_digest =
        required_lower_hex_bytes(row.get("request_sha256"), "FRED/ALFRED request digest")?;
    let lineage = required_lower_hex_bytes(
        row.get("extraction_lineage_json"),
        "FRED/ALFRED extraction lineage",
    )?;
    if request_digest.len() != 32
        || request_digest.iter().all(|byte| *byte == 0)
        || serde_json::from_slice::<Value>(&lineage)
            .ok()
            .is_none_or(|value| value.is_null())
    {
        bail!("FRED/ALFRED request or extraction-lineage evidence is invalid");
    }
    let context = observation.context();
    let provenance = context.provenance();
    let time = context.time();
    let effective = time
        .effective()
        .calendar_date_value()
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED effective date is absent"))?;
    let published = time
        .published()
        .and_then(|value| value.calendar_date_value())
        .ok_or_else(|| anyhow::anyhow!("FRED/ALFRED published date is absent"))?;
    let received_at = row_timestamp_nanos(row.get("received_at"));
    let available_at = row_timestamp_nanos(row.get("available_at"));
    let ingested_at = row_timestamp_nanos(row.get("ingested_at"));
    if row.get("schema_version").and_then(Value::as_u64) != Some(3)
        || received_at != Some(provenance.received_at().unix_nanos())
        || available_at != Some(provenance.received_at().unix_nanos())
        || ingested_at != Some(provenance.ingested_at().unix_nanos())
        || row.get("effective_date").and_then(Value::as_str) != Some(effective.to_string().as_str())
        || row.get("published_date").and_then(Value::as_str) != Some(published.to_string().as_str())
        || row.get("payload_sha256").and_then(Value::as_str)
            != Some(lower_hex(&payload_digest).as_str())
        || row.get("availability_reported_or_inferred_at").is_some()
        || row.get("availability_evidence").is_some()
        || row.get("availability_method").is_some()
        || row.get("effective_at").is_some()
        || row.get("effective_period_scheme").is_some()
        || row.get("effective_period_year").is_some()
        || row.get("effective_period_ordinal").is_some()
        || row.get("effective_period_code").is_some()
        || row.get("published_at").is_some()
        || row.get("published_period_scheme").is_some()
        || row.get("published_period_year").is_some()
        || row.get("published_period_ordinal").is_some()
        || row.get("published_period_code").is_some()
        || row.get("superseded_at").is_some()
        || row.get("superseded_period_scheme").is_some()
        || row.get("superseded_period_year").is_some()
        || row.get("superseded_period_ordinal").is_some()
        || row.get("superseded_period_code").is_some()
        || row.get("instrument_id").is_some()
        || row.get("venue_id").is_some()
        || row.get("source_timestamp").is_some()
    {
        bail!("FRED/ALFRED projected schema, time, or lineage columns differ from the payload");
    }
    match time
        .superseded()
        .and_then(|value| value.calendar_date_value())
    {
        Some(superseded)
            if row.get("superseded_precision").and_then(Value::as_str) == Some("calendar_date")
                && row.get("superseded_date").and_then(Value::as_str)
                    == Some(superseded.to_string().as_str()) => {}
        None if row.get("superseded_precision").is_none()
            && row.get("superseded_date").is_none() => {}
        Some(_) | None => {
            bail!("FRED/ALFRED projected supersession columns differ from the payload");
        }
    }

    let expected_currency = observation
        .unit()
        .as_str()
        .bytes()
        .all(|byte| byte.is_ascii_uppercase())
        .then_some(observation.unit().as_str())
        .filter(|unit| unit.len() == 3);
    if row.get("currency").and_then(Value::as_str) != expected_currency {
        bail!("FRED/ALFRED projected currency differs from the canonical unit");
    }
    match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(value), None)
            if row.get("value_state").and_then(Value::as_str) == Some("observed")
                && row.get("missing_marker").is_none()
                && row.get("missing_reason").is_none()
                && row.get("value_mantissa").and_then(json_i128) == Some(value.mantissa())
                && row.get("value_scale").and_then(Value::as_u64)
                    == Some(u64::from(value.scale())) => {}
        (None, Some(missing))
            if row.get("value_state").and_then(Value::as_str) == Some("missing")
                && row.get("missing_marker").and_then(Value::as_str)
                    == Some(missing.marker().as_str())
                && row.get("missing_reason").and_then(Value::as_str)
                    == missing.reason().map(SourceIdentifier::as_str)
                && row.get("value_mantissa").is_none()
                && row.get("value_scale").is_none() => {}
        (Some(_), None) | (None, Some(_)) | (None, None) | (Some(_), Some(_)) => {
            bail!("FRED/ALFRED projected value columns differ from the canonical payload");
        }
    }
    Ok(())
}

fn fred_row_field_allowed(field: &str) -> bool {
    matches!(
        field,
        "schema_version"
            | "request_sha256"
            | "extraction_lineage_json"
            | "observation_kind"
            | "source_id"
            | "instrument_id"
            | "venue_id"
            | "source_identifier"
            | "source_timestamp"
            | "received_at"
            | "available_at"
            | "availability_reported_or_inferred_at"
            | "availability_kind"
            | "availability_evidence"
            | "availability_method"
            | "ingested_at"
            | "effective_precision"
            | "effective_at"
            | "effective_date"
            | "effective_period_scheme"
            | "effective_period_year"
            | "effective_period_ordinal"
            | "effective_period_code"
            | "published_precision"
            | "published_at"
            | "published_date"
            | "published_period_scheme"
            | "published_period_year"
            | "published_period_ordinal"
            | "published_period_code"
            | "revision"
            | "superseded_precision"
            | "superseded_at"
            | "superseded_date"
            | "superseded_period_scheme"
            | "superseded_period_year"
            | "superseded_period_ordinal"
            | "superseded_period_code"
            | "quality"
            | "value_state"
            | "missing_marker"
            | "missing_reason"
            | "value_mantissa"
            | "value_scale"
            | "unit"
            | "currency"
            | "payload_sha256"
            | "payload_json"
    )
}

fn row_timestamp_nanos(value: Option<&Value>) -> Option<i64> {
    value
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| value.timestamp_nanos_opt())
}

fn json_i128(value: &Value) -> Option<i128> {
    value.as_number()?.to_string().parse().ok()
}

fn required_lower_hex_bytes(value: Option<&Value>, field: &str) -> Result<Vec<u8>> {
    let value = value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len().is_multiple_of(2))
        .ok_or_else(|| anyhow::anyhow!("{field} is absent or has an invalid length"))?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(value.len() / 2)
        .map_err(|_| anyhow::anyhow!("{field} allocation failed"))?;
    for pair in value.as_bytes().chunks_exact(2) {
        let high =
            lower_hex_nibble(pair[0]).ok_or_else(|| anyhow::anyhow!("{field} is not lower hex"))?;
        let low =
            lower_hex_nibble(pair[1]).ok_or_else(|| anyhow::anyhow!("{field} is not lower hex"))?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

const fn lower_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn validate_bls_publication(runtime: &Value, surface_id: &str) -> Result<()> {
    let publications = runtime
        .pointer("/publications")
        .and_then(Value::as_array)
        .filter(|publications| publications.len() == 1)
        .ok_or_else(|| anyhow::anyhow!("BLS unemployment publication evidence is absent"))?;
    let publication = publications
        .first()
        .ok_or_else(|| anyhow::anyhow!("BLS unemployment publication evidence is absent"))?;
    let provider_dataset = publication
        .get("provider_dataset")
        .and_then(Value::as_str)
        .and_then(|value| SourceIdentifier::try_from(value).ok())
        .ok_or_else(|| anyhow::anyhow!("BLS provider dataset is invalid"))?;
    let expected_prefix = match surface_id {
        "bls.v1-unregistered" => "bls:timeseries:public-v1:",
        "bls.v2-registered" => "bls:timeseries:registered-v2:",
        _ => bail!("BLS publication belongs to an unknown surface"),
    };
    let maximum_rows = match surface_id {
        "bls.v1-unregistered" => BLS_PUBLIC_MAXIMUM_ACCEPTANCE_ROWS,
        "bls.v2-registered" => BLS_REGISTERED_MAXIMUM_ACCEPTANCE_ROWS,
        _ => bail!("BLS publication belongs to an unknown surface"),
    };
    let analytical_dataset = BlsSource::analytical_dataset_identifier(&provider_dataset)
        .map_err(|_| anyhow::anyhow!("BLS provider dataset is invalid"))?;
    let source_object = publication
        .get("source_object_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("BLS source object identity is invalid"))?;
    let payload_digest = publication
        .get("source_payload_digest")
        .and_then(evidence_digest_hex)
        .ok_or_else(|| anyhow::anyhow!("BLS source payload digest is absent"))?;
    let series = publication
        .get("series_ids")
        .and_then(Value::as_array)
        .filter(|series| {
            series.len() == 1
                && series
                    .first()
                    .and_then(Value::as_str)
                    .is_some_and(|series| series == BLS_UNEMPLOYMENT_SERIES)
        })
        .ok_or_else(|| anyhow::anyhow!("BLS unemployment series evidence is invalid"))?;
    if !provider_dataset.as_str().starts_with(expected_prefix)
        || publication
            .get("analytical_dataset_id")
            .and_then(Value::as_str)
            != Some(analytical_dataset.as_str())
        || DatasetId::try_from(analytical_dataset.as_str()).is_err()
        || !bls_source_object_matches(source_object, &payload_digest)
        || publication.get("family").and_then(Value::as_str)
            != Some("bls_unemployment_rate_current_snapshot")
        || publication
            .get("temporal_semantics")
            .and_then(Value::as_str)
            != Some("locally_observed_current_snapshot")
        || series.len() != 1
        || publication
            .get("row_count")
            .and_then(Value::as_u64)
            .is_none_or(|rows| rows == 0 || rows > maximum_rows)
        || publication
            .get("observation_query_row_count")
            .and_then(Value::as_u64)
            != publication.get("row_count").and_then(Value::as_u64)
    {
        bail!("BLS unemployment publication lost direct current-snapshot provenance");
    }
    validate_research_publication(publication, surface_id, false)?;
    validate_python_training(runtime, publications, surface_id, "BLS")
}

fn validate_python_training(
    runtime: &Value,
    publications: &[Value],
    source_surface_id: &str,
    source_label: &str,
) -> Result<()> {
    let training = runtime
        .pointer("/python_training")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("{source_label} Python training evidence is absent"))?;
    let output_dataset = training
        .get("dataset_id")
        .and_then(Value::as_str)
        .filter(|value| DatasetId::try_from(*value).is_ok())
        .ok_or_else(|| anyhow::anyhow!("{source_label} Python training dataset is invalid"))?;
    let source_parent_dataset = training
        .get("source_parent_dataset_id")
        .and_then(Value::as_str)
        .filter(|value| DatasetId::try_from(*value).is_ok())
        .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parent is invalid"))?;
    let source_parent_version = training
        .get("source_parent_manifest_version")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parent is invalid"))?;
    let source_parent_hash = training
        .get("source_parent_content_hash")
        .and_then(Value::as_str)
        .filter(|value| valid_nonzero_sha256_text(value))
        .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parent is invalid"))?;
    let parents = training
        .get("parents")
        .and_then(Value::as_array)
        .filter(|parents| !parents.is_empty() && parents.len() <= MAXIMUM_TRAINING_PARENTS)
        .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parents are invalid"))?;
    let mut parent_identities = BTreeSet::new();
    for parent in parents {
        let dataset = parent
            .get("dataset_id")
            .and_then(Value::as_str)
            .filter(|value| DatasetId::try_from(*value).is_ok())
            .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parent is invalid"))?;
        let version = parent
            .get("manifest_version")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parent is invalid"))?;
        let content_hash = parent
            .get("manifest_content_hash")
            .and_then(Value::as_str)
            .filter(|value| valid_nonzero_sha256_text(value))
            .ok_or_else(|| anyhow::anyhow!("{source_label} Python training parent is invalid"))?;
        if !parent_identities.insert((dataset, version, content_hash)) {
            bail!("{source_label} Python training repeats a parent generation");
        }
    }
    let matching_publications = publications
        .iter()
        .filter(|publication| {
            publication
                .get("analytical_dataset_id")
                .and_then(Value::as_str)
                == Some(source_parent_dataset)
                && publication.get("manifest_version").and_then(Value::as_u64)
                    == Some(source_parent_version)
                && publication
                    .get("manifest_content_hash")
                    .and_then(Value::as_str)
                    == Some(source_parent_hash)
        })
        .count();
    if training.get("source_surface_id").and_then(Value::as_str) != Some(source_surface_id)
        || output_dataset == source_parent_dataset
        || matching_publications != 1
        || !parent_identities.contains(&(
            source_parent_dataset,
            source_parent_version,
            source_parent_hash,
        ))
        || training
            .get("request_byte_count")
            .and_then(Value::as_u64)
            .is_none_or(|bytes| bytes == 0 || bytes > MAXIMUM_TRAINING_REQUEST_BYTES)
        || !valid_nonzero_sha256(training.get("request_sha256"))
        || training
            .get("manifest_version")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || !valid_nonzero_sha256(training.get("manifest_content_hash"))
        || !valid_nonzero_sha256(training.get("build_spec_digest"))
        || !valid_nonzero_sha256(training.get("policy_digest"))
        || !valid_nonzero_sha256(training.get("universe_digest"))
        || !valid_nonzero_sha256(training.get("python_export_sha256"))
        || training
            .get("train_examples")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || training
            .get("validation_examples")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || training
            .get("test_examples")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
    {
        bail!("{source_label} Python training evidence is incomplete");
    }
    Ok(())
}

fn validate_research_publication(
    publication: &Value,
    expected_surface: &str,
    require_vintages: bool,
) -> Result<()> {
    let vintage_count = publication
        .get("vintage_query_row_count")
        .and_then(Value::as_u64);
    if publication.get("surface_id").and_then(Value::as_str) != Some(expected_surface)
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
        || publication
            .get("object_count")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || !valid_sha256(publication.get("lineage_digest"))
        || !publication
            .get("python_export_sha256")
            .is_some_and(Value::is_null)
        || publication
            .get("observation_query_row_count")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || (require_vintages && vintage_count.is_none_or(|value| value == 0))
        || (!require_vintages
            && !publication
                .get("vintage_query_row_count")
                .is_some_and(Value::is_null))
        || (expected_surface == "treasury.fiscal-data"
            && publication
                .get("treasury_fiscal")
                .is_none_or(Value::is_null))
        || (expected_surface != "treasury.fiscal-data"
            && publication
                .get("treasury_fiscal")
                .is_none_or(|value| !value.is_null()))
    {
        bail!("research publication evidence is incomplete");
    }
    Ok(())
}

fn bls_source_object_matches(identity: &str, payload_digest: &str) -> bool {
    let mut fields = identity.split(':');
    fields.next() == Some("bls")
        && fields.next() == Some("0")
        && fields.next() == Some(payload_digest)
        && fields.next().is_none()
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

fn treasury_fiscal_source_object_matches(
    identity: &str,
    page_number: usize,
    request_digest: &str,
    payload_digest: &str,
) -> bool {
    let mut fields = identity.split(':');
    fields.next() == Some("treasury-page")
        && fields.next() == Some("fiscal")
        && fields.next().and_then(|value| value.parse::<usize>().ok()) == Some(page_number)
        && fields.next() == Some(request_digest)
        && fields.next() == Some(payload_digest)
        && fields.next().is_none()
}

fn evidence_digest_hex(value: &Value) -> Option<String> {
    evidence_digest_bytes(value).map(|bytes| lower_hex(&bytes))
}

fn evidence_digest_bytes(value: &Value) -> Option<[u8; 32]> {
    if value.pointer("/algorithm").and_then(Value::as_str) != Some("sha256") {
        return None;
    }
    let bytes = value.pointer("/bytes")?.as_array()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (target, value) in digest.iter_mut().zip(bytes) {
        *target = value.as_u64().and_then(|value| u8::try_from(value).ok())?;
    }
    if digest == [0; 32] {
        return None;
    }
    Some(digest)
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
    value.and_then(Value::as_str).is_some_and(valid_sha256_text)
}

fn valid_nonzero_sha256(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(valid_nonzero_sha256_text)
}

fn valid_sha256_text(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_nonzero_sha256_text(value: &str) -> bool {
    valid_sha256_text(value) && value.bytes().any(|byte| byte != b'0')
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::validate_provider_surface_runtime;

    #[test]
    fn treasury_fiscal_runtime_requires_durable_publication_evidence() {
        let digest = json!({
            "algorithm": "sha256",
            "bytes": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        });
        let surface = json!({
            "activation": {
                "data_use_admission": {
                    "retrieve": true,
                    "display": true,
                    "persist": true,
                    "model_training": true,
                    "export": true,
                    "redistribute": true,
                },
            },
            "research_runtime": {
                "runtime_generation_digest": digest,
                "rights_authorization_digest": digest,
                "publications": [],
                "python_training": null,
            },
        });

        let error = validate_provider_surface_runtime("treasury.fiscal-data", &surface)
            .expect_err("Fiscal Data without a durable publication must fail closed");

        assert!(
            error
                .to_string()
                .contains("Treasury Fiscal Data publication evidence is absent"),
            "unexpected closer error: {error:#}",
        );
    }
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
