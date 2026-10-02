//! Cutoff-independent financial display coordinates over authenticated source rows.

use market_squawk_data::{
    PointInTimeRevisionState, SecResearchDisplayCoordinate, SecResearchDisplayProjector,
    SecResearchReadError, SecResearchSourceRow,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ResearchObservation, ResearchTemporalCoordinate,
};
use sha2::{Digest as _, Sha256};

use super::super::{
    company_product::{CompanyProductProjectionError, fact_envelope_bytes, project_fact},
    company_research::{CanonicalResearchReadError, company_source_row},
};

/// Advance this domain version whenever display omission, grouping or time semantics change.
pub(crate) fn financial_display_identity() -> EvidenceDigest {
    EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(b"market-squawk/investment-financial-display/v1\0").into(),
    )
}

pub(crate) struct FinancialDisplayProjection;

impl SecResearchDisplayProjector for FinancialDisplayProjection {
    fn identity(&self) -> EvidenceDigest {
        financial_display_identity()
    }

    fn project(
        &self,
        row: &SecResearchSourceRow<'_>,
        state: PointInTimeRevisionState,
    ) -> Result<Option<SecResearchDisplayCoordinate>, SecResearchReadError> {
        let observation = row.observation();
        let context = match observation {
            ResearchObservation::Fundamental(value) => value.context(),
            ResearchObservation::Filing(value) => value.context(),
            _ => return Err(SecResearchReadError::OriginMismatch),
        };
        let Some(known_at) = context
            .provenance()
            .availability()
            .conservative_available_at()
        else {
            // These rows remain in the original source and PIT exclusion audit. There is no
            // conservative display time to prepare; never promote an inferred availability.
            return Ok(None);
        };
        let (fact, filing) = company_source_row(
            row.family(),
            row.company_identity().observation(),
            row.origin().origin_digest(),
            state,
            observation.clone(),
            known_at,
        )
        .map_err(canonical_projection_error)?;
        if let Some(fact) = fact {
            let Some(fact) = project_fact(&fact, known_at).map_err(product_projection_error)?
            else {
                return Ok(None);
            };
            Ok(Some(SecResearchDisplayCoordinate::new(
                Some(fact_envelope_bytes(&fact).map_err(product_projection_error)?),
                i64::from(fact.period().end().days_since_unix_epoch()),
                None,
                fact.filed_on()
                    .map(|date| i64::from(date.days_since_unix_epoch())),
                None,
            )))
        } else if let Some(filing) = filing {
            let (effective_day, effective_time) = display_time(filing.effective())?;
            let published = filing.published().map(display_time).transpose()?;
            Ok(Some(SecResearchDisplayCoordinate::new(
                None,
                effective_day,
                effective_time,
                published.map(|(day, _)| day),
                published.and_then(|(_, time)| time),
            )))
        } else {
            Err(SecResearchReadError::OriginMismatch)
        }
    }
}

/// Calendar precision stays a day; exact timestamps retain their within-day ordering.
/// This is only a display key and never changes a source time or selection cutoff.
fn display_time(
    value: &ResearchTemporalCoordinate,
) -> Result<(i64, Option<i64>), SecResearchReadError> {
    if let Some(date) = value.calendar_date_value() {
        Ok((i64::from(date.days_since_unix_epoch()), None))
    } else if let Some(timestamp) = value.exact_timestamp() {
        let nanos = timestamp.unix_nanos();
        Ok((nanos.div_euclid(86_400_000_000_000), Some(nanos)))
    } else {
        Err(SecResearchReadError::OriginMismatch)
    }
}

fn canonical_projection_error(error: CanonicalResearchReadError) -> SecResearchReadError {
    match error {
        CanonicalResearchReadError::Cancelled => SecResearchReadError::Cancelled,
        CanonicalResearchReadError::DeadlineExceeded => SecResearchReadError::DeadlineExceeded,
        CanonicalResearchReadError::AuthorityUnavailable => {
            SecResearchReadError::AuthorityUnavailable
        }
        CanonicalResearchReadError::ResourceExhausted => SecResearchReadError::ObjectBudgetExceeded,
        CanonicalResearchReadError::InvalidRequest
        | CanonicalResearchReadError::EvidenceConflict
        | CanonicalResearchReadError::RestartConflict => SecResearchReadError::OriginMismatch,
    }
}

fn product_projection_error(error: CompanyProductProjectionError) -> SecResearchReadError {
    match error {
        CompanyProductProjectionError::ResourceExhausted => {
            SecResearchReadError::ObjectBudgetExceeded
        }
        CompanyProductProjectionError::InvalidEvidence => SecResearchReadError::OriginMismatch,
    }
}
