//! Saved reference zones in the authenticated original chart share frame.

use super::nanos;
use market_squawk_data::ForecastBasisHistory;
use market_squawk_decisions::{InvestmentProposalDecision, TargetPriceRange};
use market_squawk_domain::Timestamp;
use market_squawk_services::ServiceError;
use serde_json::{Value, json};

/// The decision retains a source-minted conversion (reconstructed by the saved decision reader),
/// while `history` physically reopens the original chart sources. Neither a persisted scalar nor
/// matching nominal currency alone establishes that these two price frames are comparable.
/// Expiry bounds historical display; this function never reauthorizes a current action.
pub(super) fn value(
    decision: &InvestmentProposalDecision,
    history: Option<&ForecastBasisHistory>,
) -> Result<Value, ServiceError> {
    let unavailable = |reason, summary| unavailable(decision, reason, summary);
    let InvestmentProposalDecision::Generated(proposal) = decision else {
        return Ok(unavailable(
            "no_supported_action_ranges",
            "This saved analysis did not support action ranges.",
        ));
    };
    let evidence = proposal.evidence();
    let Some(projection) = evidence.current_share_projection() else {
        return Ok(unavailable(
            "share_conversion_unavailable",
            "The saved action ranges do not establish a conversion into this chart's share units.",
        ));
    };
    let Some(history) = history else {
        return Ok(unavailable(
            "original_history_unavailable",
            "The original price history must be reopened before saved action ranges can be compared with it.",
        ));
    };
    let conversion = projection.conversion();
    let original = projection.original_forecast();
    let admission = projection.market_admission();
    if history.instrument_id() != evidence.instrument_id()
        || history.origin_price().currency() != evidence.currency()
        || history.basis_identity() != conversion.original_basis_identity()
        || history.origin_at() != original.window().observed_at()
        || history.source_cutoff() > evidence.as_of()
        || conversion.instrument_id() != evidence.instrument_id()
        || conversion.currency() != evidence.currency()
        || conversion.quote_at() < history.origin_at()
        || conversion.market_cutoff() > evidence.admitted_at()
        || conversion.knowledge_cutoff() > evidence.admitted_at()
        || evidence.market() != Some(&admission.market)
        || admission.authorized_at > evidence.admitted_at()
        || evidence.as_of() > evidence.admitted_at()
        || proposal.price_ladder().current_share_basis() != Some(projection.identity())
    {
        return Err(ServiceError::InvalidResult);
    }
    // The proposal authority already intersects every evidence expiry and freshness limit.
    // Keep the source authorization and horizon explicit, and never start at the earlier
    // research cutoff: operational evidence may only become known at admission.
    let start = evidence.admitted_at();
    let end = proposal
        .expires_at()
        .min(proposal.horizon_at())
        .min(admission.authorization_expires_at);
    if start >= end {
        return Ok(unavailable(
            "expired_at_admission",
            "The saved action ranges had no eligible interval at the original admission time.",
        ));
    }
    let Ok(overlay) = proposal.original_share_action_overlay() else {
        return Ok(unavailable(
            "range_conversion_unavailable",
            "The saved action ranges cannot be represented in the original chart share units without losing their financial ordering.",
        ));
    };
    if overlay.original_basis_identity().evidence_digest().bytes()
        != history.basis_identity().bytes()
        || overlay.projection_identity() != projection.identity()
    {
        return Err(ServiceError::InvalidResult);
    }
    let ranges = [
        range(
            "entry",
            "Saved buy reference",
            overlay.entry_range(),
            start,
            end,
            "Saved entry reference zone. The original buy condition also required no existing position and a price above the exit ceiling; this band alone is not an instruction to buy.",
        ),
        range(
            "add",
            "Saved add reference",
            overlay.add_range(),
            start,
            end,
            "Saved add reference zone. The original add condition also required an existing position and a price above the exit ceiling; this band alone is not an instruction to add.",
        ),
        range(
            "trim",
            "Saved trim reference",
            overlay.trim_range(),
            start,
            end,
            "Saved trim reference zone. The original trim condition began at the lower boundary and could extend above this band; it required an existing position.",
        ),
        range(
            "exit",
            "Saved exit reference",
            overlay.exit_range(),
            start,
            end,
            "Saved downside exit reference zone. The original sell condition included prices at or below its upper boundary and required an existing position.",
        ),
    ];
    Ok(json!({
        "state":"available", "basis":"split_adjusted_price",
        "summary":"Saved reference zones in the original chart's split-adjusted share units, rounded outward by the decision authority. They apply only from original admission up to, but not including, expiry; they do not establish current trading eligibility.",
        "informationCurrentThroughUnixNanos":nanos(evidence.as_of()),
        "admittedAtUnixNanos":nanos(start),
        "expiresAtUnixNanos":nanos(end),
        "ranges":ranges,
    }))
}

fn range(
    kind: &str,
    label: &str,
    value: TargetPriceRange,
    start: Timestamp,
    end: Timestamp,
    summary: &str,
) -> Value {
    json!({
        "kind":kind, "label":label,
        "lower":value.lower().amount().normalize().to_string(),
        "upper":value.upper().amount().normalize().to_string(),
        "startAtUnixNanos":nanos(start), "endAtUnixNanos":nanos(end),
        "summary":summary,
    })
}

fn unavailable(decision: &InvestmentProposalDecision, reason: &str, summary: &str) -> Value {
    json!({
        "state":"unavailable", "basis":"split_adjusted_price",
        "reason":reason, "summary":summary,
        "informationCurrentThroughUnixNanos":nanos(decision.evidence().as_of()),
        "admittedAtUnixNanos":nanos(decision.evidence().admitted_at()),
        "expiresAtUnixNanos":nanos(decision.expires_at().min(decision.horizon_at())),
        "ranges":[],
    })
}
