//! Exact annual government-reference rate plus independently evidenced risk premium.

use crate::{
    AutomaticValuationAssumption, AutomaticValuationAssumptionKind, AutomaticValuationError,
};
use market_squawk_data::DatasetManifestRef;
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

/// Government-reference maturity; neither maturity denotes a zero-coupon discount curve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MacroRateMaturity {
    /// Ten-year annual government yield.
    TenYear,
    /// Thirty-year annual government yield.
    ThirtyYear,
}

/// Exact selected government yield and the consumer policy that bounds its use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MacroRateReferenceEvidence {
    maturity: MacroRateMaturity,
    annual_yield_percent: Decimal,
    context_identity: EvidenceDigest,
    evidence_identity: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    available_at: Timestamp,
    expires_at: Timestamp,
}

impl MacroRateReferenceEvidence {
    /// Binds a normalized percent-per-year observation to its exact selection and lifetime.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact rate selection and clocks are independent"
    )]
    pub fn try_new(
        maturity: MacroRateMaturity,
        annual_yield_percent: Decimal,
        context_identity: EvidenceDigest,
        evidence_identity: EvidenceDigest,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
        available_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        if available_at > knowledge_cutoff
            || expires_at <= knowledge_cutoff
            || [context_identity, evidence_identity]
                .iter()
                .any(|id| id.algorithm() != DigestAlgorithm::Sha256 || id.bytes() == [0; 32])
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(Self {
            maturity,
            annual_yield_percent: annual_yield_percent.normalize(),
            context_identity,
            evidence_identity,
            knowledge_cutoff,
            effective_date_cutoff,
            available_at,
            expires_at,
        })
    }

    #[must_use]
    pub const fn maturity(self) -> MacroRateMaturity {
        self.maturity
    }
    #[must_use]
    pub const fn annual_yield_percent(self) -> Decimal {
        self.annual_yield_percent
    }
    #[must_use]
    pub const fn context_identity(self) -> EvidenceDigest {
        self.context_identity
    }
    #[must_use]
    pub const fn evidence_identity(self) -> EvidenceDigest {
        self.evidence_identity
    }
    #[must_use]
    pub const fn knowledge_cutoff(self) -> Timestamp {
        self.knowledge_cutoff
    }
    #[must_use]
    pub const fn effective_date_cutoff(self) -> CalendarDate {
        self.effective_date_cutoff
    }
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }
    #[must_use]
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }
}

/// Causal rate assumption used by an annual-period DCF or residual-income calculation.
///
/// The reference remains a par yield. The explicit annual risk premium has separate evidence;
/// the final annual rate is exactly `reference_percent / 100 + premium_fraction`. This contract
/// never infers a premium or turns a bare government yield into an equity discount rate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinancialModelMacroAssumptions {
    reference: MacroRateReferenceEvidence,
    premium: AutomaticValuationAssumption,
    assumption: AutomaticValuationAssumption,
    premium_source: Option<Box<[u8]>>,
    premium_parents: Box<[DatasetManifestRef]>,
}

impl FinancialModelMacroAssumptions {
    /// Derives the exact assumption that must be passed into the valuation calculator.
    pub fn try_new(
        reference: MacroRateReferenceEvidence,
        premium: AutomaticValuationAssumption,
        rate_kind: AutomaticValuationAssumptionKind,
        identifier: &str,
    ) -> Result<Self, AutomaticValuationError> {
        if !matches!(
            rate_kind,
            AutomaticValuationAssumptionKind::DiscountRate
                | AutomaticValuationAssumptionKind::CostOfEquity
        ) || premium.kind() != rate_kind
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        let hundred = Decimal::from(100_u32);
        let reference_fraction = reference
            .annual_yield_percent
            .checked_div(hundred)
            .ok_or(AutomaticValuationError::Arithmetic)?;
        if reference_fraction.checked_mul(hundred) != Some(reference.annual_yield_percent) {
            return Err(AutomaticValuationError::Arithmetic);
        }
        let rate = reference_fraction
            .checked_add(premium.value())
            .ok_or(AutomaticValuationError::Arithmetic)?;
        if rate.checked_sub(premium.value()) != Some(reference_fraction)
            || rate.checked_sub(reference_fraction) != Some(premium.value())
        {
            return Err(AutomaticValuationError::Arithmetic);
        }
        let available_at = reference.available_at.max(premium.available_at());
        let expires_at = reference.expires_at.min(premium.expires_at());
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/annual-model-macro-rate-assumption/v1\0");
        hash.update([match reference.maturity {
            MacroRateMaturity::TenYear => 1,
            MacroRateMaturity::ThirtyYear => 2,
        }]);
        hash.update(reference.annual_yield_percent.mantissa().to_be_bytes());
        hash.update(reference.annual_yield_percent.scale().to_be_bytes());
        hash.update(reference.context_identity.bytes());
        hash.update(reference.evidence_identity.bytes());
        hash.update(reference.knowledge_cutoff.unix_nanos().to_be_bytes());
        hash.update(reference.effective_date_cutoff.to_string().as_bytes());
        hash.update(reference.available_at.unix_nanos().to_be_bytes());
        hash.update(reference.expires_at.unix_nanos().to_be_bytes());
        hash.update(automatic_assumptions_identity(std::slice::from_ref(&premium))?.bytes());
        let evidence = checked_digest(hash.finalize().into())?;
        let assumption = AutomaticValuationAssumption::try_new(
            rate_kind,
            identifier,
            rate,
            evidence,
            available_at,
            expires_at,
        )
        .map_err(|_| AutomaticValuationError::InvalidContract)?;
        Ok(Self {
            reference,
            premium,
            assumption,
            premium_source: None,
            premium_parents: Box::new([]),
        })
    }

    /// Retains the source owner's exact bounded replay recipe and complete macro source roots.
    /// This binds a replay locator; only the actual application source reader authenticates it.
    /// Replacing an already bound recipe is rejected.
    pub fn try_with_premium_source(
        mut self,
        reference_bytes: Box<[u8]>,
        parent_manifests: Vec<DatasetManifestRef>,
    ) -> Result<Self, AutomaticValuationError> {
        if self.premium_source.is_some()
            || reference_bytes.is_empty()
            || reference_bytes.len() > 64 * 1024
            || parent_manifests.is_empty()
            || parent_manifests.len() > 64
            || parent_manifests
                .iter()
                .enumerate()
                .any(|(index, value)| parent_manifests[..index].contains(value))
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        let mut hash = crate::CanonicalHasher::new(b"market-squawk/annual-macro-premium-source/v1");
        hash.fixed(self.assumption.evidence().bytes());
        hash.bytes(&reference_bytes);
        hash.u64(
            u64::try_from(parent_manifests.len())
                .map_err(|_| AutomaticValuationError::Arithmetic)?,
        );
        for manifest in &parent_manifests {
            crate::evidence::hash_manifest(&mut hash, manifest);
        }
        self.assumption = AutomaticValuationAssumption::try_new(
            self.assumption.kind(),
            self.assumption.identifier(),
            self.assumption.value(),
            checked_digest(hash.finish())?,
            self.assumption.available_at(),
            self.assumption.expires_at(),
        )?;
        self.premium_source = Some(reference_bytes);
        self.premium_parents = parent_manifests.into_boxed_slice();
        Ok(self)
    }

    /// Original bounded source recipe, to be reopened by its actual installed source owner.
    pub fn premium_source_reference(&self) -> Option<&[u8]> {
        self.premium_source.as_deref()
    }
    /// Complete original macro and premium input roots, admitted together for this calculation.
    pub fn premium_parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.premium_parents
    }

    pub(crate) fn revalidated(&self) -> Result<Self, AutomaticValuationError> {
        let value = Self::try_new(
            self.reference,
            self.premium.clone(),
            self.assumption.kind(),
            self.assumption.identifier(),
        )?;
        match self.premium_source.as_ref() {
            Some(bytes) => {
                value.try_with_premium_source(bytes.clone(), self.premium_parents.to_vec())
            }
            None if self.premium_parents.is_empty() => Ok(value),
            None => Err(AutomaticValuationError::InvalidContract),
        }
    }

    #[must_use]
    pub const fn reference(&self) -> MacroRateReferenceEvidence {
        self.reference
    }
    #[must_use]
    pub const fn premium(&self) -> &AutomaticValuationAssumption {
        &self.premium
    }
    /// Exact value, identity, availability and expiry that the model must actually consume.
    #[must_use]
    pub const fn assumption(&self) -> &AutomaticValuationAssumption {
        &self.assumption
    }
}

/// Commits the exact ordered model assumptions with the existing canonical digest policy.
pub fn automatic_assumptions_identity(
    assumptions: &[AutomaticValuationAssumption],
) -> Result<EvidenceDigest, AutomaticValuationError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/financial-model-assumptions/v1\0");
    digest.update(
        u64::try_from(assumptions.len())
            .map_err(|_| AutomaticValuationError::Arithmetic)?
            .to_be_bytes(),
    );
    for assumption in assumptions {
        digest.update([automatic_assumption_kind_tag(assumption.kind())]);
        let identifier = assumption.identifier().as_bytes();
        digest.update(
            u64::try_from(identifier.len())
                .map_err(|_| AutomaticValuationError::Arithmetic)?
                .to_be_bytes(),
        );
        digest.update(identifier);
        let value = assumption.value().normalize();
        digest.update(value.mantissa().to_be_bytes());
        digest.update(value.scale().to_be_bytes());
        let evidence = assumption.evidence();
        if evidence.algorithm() != DigestAlgorithm::Sha256 || evidence.bytes() == [0; 32] {
            return Err(AutomaticValuationError::InvalidContract);
        }
        digest.update(evidence.bytes());
        digest.update(assumption.available_at().unix_nanos().to_be_bytes());
        digest.update(assumption.expires_at().unix_nanos().to_be_bytes());
    }
    checked_digest(digest.finalize().into())
}

const fn automatic_assumption_kind_tag(kind: AutomaticValuationAssumptionKind) -> u8 {
    match kind {
        AutomaticValuationAssumptionKind::DiscountRate => 1,
        AutomaticValuationAssumptionKind::ComparableWeight => 2,
        AutomaticValuationAssumptionKind::CostOfEquity => 3,
        AutomaticValuationAssumptionKind::ForecastProbability => 4,
        AutomaticValuationAssumptionKind::UncertaintyLower => 5,
        AutomaticValuationAssumptionKind::UncertaintyUpper => 6,
        AutomaticValuationAssumptionKind::TerminalGrowth => 7,
    }
}

fn checked_digest(bytes: [u8; 32]) -> Result<EvidenceDigest, AutomaticValuationError> {
    if bytes == [0; 32] {
        return Err(AutomaticValuationError::InvalidContract);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}
