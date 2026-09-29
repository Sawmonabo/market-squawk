//! Actual default premium consumption through the one canonical annual macro-rate receipt.

use super::*;
use crate::application::research::HistoricalEquityPremiumRead;

/// Uses only the source-issued premium and the same-cutoff current ten-year reference. The
/// resulting canonical receipt retains the exact bounded replay recipe and every premium parent;
/// existing automatic DCF/residual requests must consume its `assumption()` and its source roots.
/// The implied unscaled market/beta-one risk convention is disclosed in the estimator policy.
pub(crate) fn derive_default_financial_model_macro_assumptions(
    context: &MacroInvestmentContext,
    premium: &HistoricalEquityPremiumRead,
    rate_kind: AutomaticValuationAssumptionKind,
    identifier: &str,
    maximum_age_nanos: NonZeroU64,
) -> Result<FinancialModelMacroAssumptions, RecommendationFinancialModelAdapterError> {
    if context.knowledge_cutoff() != premium.government().reference().knowledge_cutoff() {
        return Err(RecommendationFinancialModelAdapterError::MacroContextMismatch);
    }
    let annual_premium = premium
        .assumption(rate_kind)
        .map_err(|_| RecommendationFinancialModelAdapterError::InvalidProposalEvidence)?;
    let reference = premium
        .canonical_reference_bytes()
        .map_err(|_| RecommendationFinancialModelAdapterError::InvalidProposalEvidence)?;
    let mut parents = premium.parent_manifests().to_vec();
    parents.extend_from_slice(context.parent_manifests());
    parents.sort_by(|left, right| {
        left.dataset_id()
            .as_str()
            .cmp(right.dataset_id().as_str())
            .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
    });
    if parents.windows(2).any(|pair| {
        pair[0].dataset_id() == pair[1].dataset_id()
            && pair[0].manifest_version() == pair[1].manifest_version()
            && pair[0] != pair[1]
    }) {
        return Err(RecommendationFinancialModelAdapterError::MacroContextMismatch);
    }
    parents.dedup();
    derive_financial_model_macro_assumptions(
        context,
        MacroRateMaturity::TenYear,
        annual_premium,
        rate_kind,
        identifier,
        maximum_age_nanos,
    )?
    .try_with_premium_source(reference, parents)
    .map_err(|_| RecommendationFinancialModelAdapterError::InvalidProposalEvidence)
}
