//! Source-derived single-common-class share units, separate from price-forecast units.

use super::*;
use market_squawk_data::{
    CorporateActionPlan, SecResearchDisposition, SecResearchFamily, SecResearchIdentityOutcome,
    SecResearchIdentitySelection,
};
use market_squawk_domain::{
    CalendarDate, CorporateActionKind, ResearchObservation, XbrlPeriod, XbrlQualifiedName,
};
use sha2::Digest as _;

/// The explicitly conditional basic-share interpretation; this is not a dilution forecast.
pub const REPORTED_COMMON_SHARE_ASSUMPTION: &str = "This value uses the company's latest reported share count. It assumes no later net increase in shares and does not include potential shares from options or convertible securities.";

/// A positive single-common-class count from a complete, physically verified filing.
/// There is no scalar or deserialization constructor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommonShareFilingEvidence {
    identity: EvidenceDigest,
    instrument: InstrumentId,
    issuer: [u8; 32],
    accession: [u8; 32],
    annual_eps: Option<(CalendarDate, CalendarDate, Decimal, Currency)>,
    shares: Decimal,
    reported_on: CalendarDate,
    knowledge_at: Timestamp,
}
impl CommonShareFilingEvidence {
    pub fn try_from_filing(
        selection: &SecResearchIdentitySelection,
    ) -> Result<Self, AutomaticValuationError> {
        let unavailable =
            AutomaticValuationError::Unavailable(AutomaticValuationUnavailable::MethodInput);
        let SecResearchIdentityOutcome::Exact(selected) = selection.outcome() else {
            return Err(unavailable);
        };
        let filing = selected.filing_xbrl().ok_or(unavailable)?;
        if selection.request().family() != SecResearchFamily::FilingXbrl
            || selection.identity().disposition() != CompanySecurityIdentityDisposition::Complete
            || selection.identity().candidates().len() != 1
            || selected.disposition() != SecResearchDisposition::Selected
            || selected.request().knowledge_at() != selection.request().knowledge_at()
            || !selected.conflicts().is_empty()
        {
            return Err(unavailable);
        }
        let company = selected.company_identity().observation();
        selection
            .identity()
            .receipt()
            .validate_selected_company(
                selection.request().instrument_id(),
                company,
                selected.receipt().company_observation_digest(),
                selection.request().knowledge_at(),
            )
            .map_err(|_| unavailable)?;
        if company.surface() != market_squawk_domain::CompanyIdentitySurface::SecFilingXbrl
            || company.provider_company_id().as_str() != filing.cik()
        {
            return Err(unavailable);
        }
        for fact in filing.nonnumeric_occurrences().iter() {
            if common_count(fact.map_err(|_| unavailable)?.concept()) {
                return Err(unavailable);
            }
        }
        validate_single_common_listing(filing)?;
        let mut found = None;
        let mut annual_eps = None;
        // Inspect the complete occurrence set, including excluded rows. PIT exclusion cannot
        // conceal a conflicting class. Equivalent repeated tags are not additional classes.
        for (ordinal, row) in selected.decoded_rows().iter().enumerate() {
            let row = row.map_err(|_| unavailable)?;
            let ResearchObservation::Fundamental(fact) = row else {
                return Err(unavailable);
            };
            crate::evidence::validate_fundamental_company(&fact, company)
                .map_err(|_| unavailable)?;
            let xbrl = fact.xbrl_evidence().ok_or(unavailable)?;
            if xbrl.concept().local_name().as_str() == "EarningsPerShareDiluted"
                && xbrl
                    .concept()
                    .namespace_uri()
                    .is_some_and(|uri| uri.as_str().starts_with("http://fasb.org/us-gaap/"))
                && let XbrlPeriod::Duration { start, end } = xbrl.period()
                && Some(end) == filing.report_date()
                && (364..=371)
                    .contains(&(end.days_since_unix_epoch() - start.days_since_unix_epoch() + 1))
            {
                let (numerator, denominator) = xbrl.unit().divide_parts().ok_or(unavailable)?;
                let ([currency], [shares]) = (numerator, denominator) else {
                    return Err(unavailable);
                };
                if !xbrl.dimensions().is_empty()
                    || currency.namespace_uri().map(|s| s.as_str())
                        != Some("http://www.xbrl.org/2003/iso4217")
                    || shares.namespace_uri().map(|s| s.as_str())
                        != Some("http://www.xbrl.org/2003/instance")
                    || shares.local_name().as_str() != "shares"
                {
                    return Err(unavailable);
                }
                let value = (
                    start,
                    end,
                    fact.value(),
                    Currency::try_from(currency.local_name().as_str()).map_err(|_| unavailable)?,
                );
                if annual_eps.is_some_and(|prior| prior != value) {
                    return Err(unavailable);
                }
                annual_eps = Some(value);
            }
            if !common_count(xbrl.concept()) {
                continue;
            }
            let XbrlPeriod::Instant { instant } = xbrl.period() else {
                return Err(unavailable);
            };
            let unit = xbrl.unit().measure_name().ok_or(unavailable)?;
            if !xbrl.dimensions().is_empty()
                || !xbrl.context_graph().events().is_empty()
                || unit.local_name().as_str() != "shares"
                || unit.namespace_uri().map(|s| s.as_str())
                    != Some("http://www.xbrl.org/2003/instance")
                || xbrl.accession() != filing.accession()
                || xbrl.entity().scheme().as_str() != "http://www.sec.gov/CIK"
                || xbrl.entity().value().as_str() != filing.cik()
                || fact.value() <= Decimal::ZERO
                || !fact.value().fract().is_zero()
                || instant
                    > selection
                        .request()
                        .knowledge_at()
                        .utc_calendar_date()
                        .map_err(|_| unavailable)?
                || filing.report_date().is_none_or(|end| instant < end)
                || !selected
                    .selected()
                    .iter()
                    .any(|row| usize::try_from(row.row().row_ordinal()).ok() == Some(ordinal))
            {
                return Err(unavailable);
            }
            let value = (fact.value().normalize(), instant);
            if found.is_some_and(|prior| prior != value) {
                return Err(unavailable);
            }
            found = Some(value);
        }
        let (shares, reported_on) = found.ok_or(unavailable)?;
        let issuer: [u8; 32] = sha2::Sha256::digest(filing.cik().as_bytes()).into();
        let mut hash = CanonicalHasher::new(b"market-squawk/reported-single-common-class/v1");
        hash.fixed(selected.receipt().result_digest().bytes());
        hash.fixed(selection.identity().receipt().receipt_digest().bytes());
        hash.fixed(filing.sidecar_digest().bytes());
        hash.bytes(REPORTED_COMMON_SHARE_ASSUMPTION.as_bytes());
        Ok(Self {
            identity: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish()),
            instrument: selection.request().instrument_id(),
            issuer,
            accession: sha2::Sha256::digest(filing.accession().as_str().as_bytes()).into(),
            annual_eps,
            shares,
            reported_on,
            knowledge_at: selection.request().knowledge_at(),
        })
    }
    pub const fn identity(self) -> EvidenceDigest {
        self.identity
    }
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument
    }
    pub const fn shares(self) -> Decimal {
        self.shares
    }
    pub const fn reported_on(self) -> CalendarDate {
        self.reported_on
    }
    pub const fn knowledge_at(self) -> Timestamp {
        self.knowledge_at
    }
}
fn common_count(name: &XbrlQualifiedName) -> bool {
    name.local_name().as_str() == "EntityCommonStockSharesOutstanding"
        && name
            .namespace_uri()
            .is_some_and(|uri| uri.as_str().starts_with("http://xbrl.sec.gov/dei/"))
}

/// A filing's common share frame with complete source coverage through the exact quote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommonShareValuationBasis {
    filing: CommonShareFilingEvidence,
    identity: EvidenceDigest,
    starts_on: CalendarDate,
    quote_at: Timestamp,
}
impl CommonShareValuationBasis {
    pub fn try_from_source_plan(
        filing: CommonShareFilingEvidence,
        plan: &CorporateActionPlan,
        starts_on: CalendarDate,
        quote_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        let unavailable =
            AutomaticValuationError::Unavailable(AutomaticValuationUnavailable::MethodInput);
        let coverage = plan.source_split_admission().ok_or(unavailable)?;
        // Let the source owner validate the quote against its native calendar bound. UTC date
        // arithmetic would reject US evening quotes whose UTC day is already the following day.
        plan.source_split_projection_limits(
            market_squawk_data::CorporateActionPolicy::new(
                market_squawk_data::CorporateActionAdjustment::SplitAdjusted,
                plan.policy().version(),
            ),
            filing.instrument,
            plan.knowledge_cutoff(),
            quote_at,
        )
        .map_err(|_| unavailable)?;
        if starts_on > filing.reported_on
            || starts_on > coverage.interval().1
            || coverage.interval().0 > starts_on
            || !coverage.instruments().contains(&filing.instrument)
            || plan.valuation_cutoff() != quote_at
            || plan.knowledge_cutoff() < filing.knowledge_at
            || coverage.application_starts_at(filing.instrument).is_none()
            || plan.admitted().iter().any(|record| {
                record.observation().context().provenance().instrument_id()
                    == Some(filing.instrument)
                    // A plan can cover the earlier EPS period as well. A split
                    // preceding this method's basis does not change the later count.
                    && record.observation().context().time().effective()
                        .calendar_date_value().is_none_or(|date| date >= starts_on)
                    && !matches!(
                        record.observation().action(),
                        CorporateActionKind::CashDividend { .. }
                    )
            })
        {
            return Err(unavailable);
        }
        let mut hash =
            CanonicalHasher::new(b"market-squawk/unchanged-filing-common-share-frame/v1");
        hash.fixed(filing.identity.bytes());
        hash.fixed(plan.content_hash().bytes());
        hash.fixed(plan.audit_hash().bytes());
        hash.fixed(coverage.evidence_digest().bytes());
        hash.i64(i64::from(starts_on.days_since_unix_epoch()));
        hash.i64(quote_at.unix_nanos());
        Ok(Self {
            filing,
            identity: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish()),
            starts_on,
            quote_at,
        })
    }
    pub const fn identity(self) -> EvidenceDigest {
        self.identity
    }
    pub const fn filing(self) -> CommonShareFilingEvidence {
        self.filing
    }
}

impl AutomaticValuationMethodReceipt {
    /// Exact instruments whose reported share frames must be admitted for this method.
    pub fn common_share_instruments(&self) -> Result<Vec<InstrumentId>, AutomaticValuationError> {
        verify_recovered_receipt(self)?;
        if self.method == AutomaticValuationMethod::ForecastDistribution {
            return Ok(Vec::new());
        }
        let mut instruments = vec![self.instrument_id];
        for peer in &self.peer_identities {
            let [candidate] = peer.ordered_candidates() else {
                return Err(AutomaticValuationError::InvalidContract);
            };
            instruments.push(candidate.instrument_id());
        }
        if instruments.len() > 17 {
            return Err(AutomaticValuationError::InvalidContract);
        }
        Ok(instruments)
    }
    /// Financial source start and actual quote; these are preparation requirements, not authority.
    pub fn common_share_source_requirement(
        &self,
        filing: CommonShareFilingEvidence,
    ) -> Result<(InstrumentId, CalendarDate, Timestamp), AutomaticValuationError> {
        let invalid = AutomaticValuationError::InvalidContract;
        if !self
            .common_share_instruments()?
            .contains(&filing.instrument)
            || filing.knowledge_at != self.measurement_at
        {
            return Err(invalid);
        }
        let identity = if filing.instrument == self.instrument_id {
            &self.company_security
        } else {
            self.peer_identities
                .iter()
                .find(|r| {
                    r.ordered_candidates()
                        .iter()
                        .any(|c| c.instrument_id() == filing.instrument)
                })
                .ok_or(invalid)?
        };
        let [candidate] = identity.ordered_candidates() else {
            return Err(invalid);
        };
        let issuer: [u8; 32] =
            sha2::Sha256::digest(candidate.provider_company_id().as_str().as_bytes()).into();
        if issuer != filing.issuer {
            return Err(invalid);
        }
        let mut markets = self.inputs.iter().filter(|input| {
            input.input.reference_instrument_id() == filing.instrument
                && matches!(
                    input.input.evidence().origin(),
                    EvidenceOrigin::PublishedMarket { .. }
                )
        });
        let quote = markets
            .next()
            .ok_or(invalid)?
            .input
            .evidence()
            .effective_at()
            .ok_or(invalid)?;
        if markets.next().is_some() {
            return Err(invalid);
        }
        let mut start = filing.reported_on;
        if self.method == AutomaticValuationMethod::ComparableCompanies {
            let mut metrics = self.inputs.iter().filter(|input| {
                input.input.reference_instrument_id() == filing.instrument
                    && matches!(
                        input.input.evidence().origin(),
                        EvidenceOrigin::Fundamental { .. }
                    )
            });
            let metric = metrics.next().ok_or(invalid)?;
            if metrics.next().is_some() {
                return Err(invalid);
            }
            let EvidenceOrigin::Fundamental {
                canonical_observation,
                ..
            } = metric.input.evidence().origin()
            else {
                return Err(invalid);
            };
            let ResearchObservation::Fundamental(fact) =
                serde_json::from_str(canonical_observation).map_err(|_| invalid)?
            else {
                return Err(invalid);
            };
            let FundamentalPeriod::Duration {
                start: period_start,
                end,
            } = fact.fact_context().period()
            else {
                return Err(invalid);
            };
            let accession: [u8; 32] =
                sha2::Sha256::digest(fact.fact_context().accession().as_str().as_bytes()).into();
            if accession != filing.accession
                || filing.annual_eps
                    != Some((
                        period_start,
                        end,
                        fact.value(),
                        metric.input.amount().money().currency(),
                    ))
            {
                return Err(AutomaticValuationError::Unavailable(
                    AutomaticValuationUnavailable::MethodInput,
                ));
            }
            start = start.min(period_start);
        }
        Ok((filing.instrument, start, quote))
    }
    pub(super) fn validate_common_share_bases(
        &self,
        bases: &[CommonShareValuationBasis],
    ) -> Result<Decimal, AutomaticValuationError> {
        let instruments = self.common_share_instruments()?;
        if bases.len() != instruments.len() {
            return Err(AutomaticValuationError::InvalidContract);
        }
        for (instrument, basis) in instruments.iter().zip(bases) {
            let expected = self.common_share_source_requirement(basis.filing)?;
            if *instrument != basis.filing.instrument
                || expected != (*instrument, basis.starts_on, basis.quote_at)
            {
                return Err(AutomaticValuationError::InvalidContract);
            }
        }
        Ok(
            if self.method == AutomaticValuationMethod::ComparableCompanies {
                Decimal::ONE
            } else {
                bases
                    .first()
                    .ok_or(AutomaticValuationError::InvalidContract)?
                    .filing
                    .shares
            },
        )
    }
}

/// Divide a positive total by an integral source count, rounding only the final result.
pub(super) fn divide_common_shares(
    amount: Decimal,
    shares: Decimal,
    scale: u32,
    rounding: market_squawk_data::ShareConversionRounding,
) -> Result<Decimal, AutomaticValuationError> {
    let error = AutomaticValuationError::Arithmetic;
    if amount <= Decimal::ZERO || shares <= Decimal::ZERO || !shares.fract().is_zero() || scale > 28
    {
        return Err(error);
    }
    let mut numerator = u128::try_from(amount.mantissa()).map_err(|_| error)?;
    let mut denominator = u128::try_from(shares.normalize().mantissa()).map_err(|_| error)?;
    // Cancel first, so genuine large issuer totals do not overflow intermediate multiplication.
    let mut a = numerator;
    let mut b = denominator;
    while b != 0 {
        (a, b) = (b, a % b);
    }
    numerator /= a;
    denominator /= a;
    if scale >= amount.scale() {
        numerator = numerator
            .checked_mul(10_u128.checked_pow(scale - amount.scale()).ok_or(error)?)
            .ok_or(error)?;
    } else {
        denominator = denominator
            .checked_mul(10_u128.checked_pow(amount.scale() - scale).ok_or(error)?)
            .ok_or(error)?;
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let increment = match rounding {
        market_squawk_data::ShareConversionRounding::Lower => false,
        market_squawk_data::ShareConversionRounding::Upper => remainder != 0,
        market_squawk_data::ShareConversionRounding::Central => {
            remainder > denominator - remainder
                || (remainder == denominator - remainder && quotient % 2 == 1)
        }
    };
    Decimal::try_from_i128_with_scale(
        i128::try_from(quotient.checked_add(u128::from(increment)).ok_or(error)?)
            .map_err(|_| error)?,
        scale,
    )
    .map_err(|_| error)
}

// The sole common-class count must agree with the filing's complete registered-security family.
// Unknown/custom equity classes remain unavailable; extra clearly described debt is allowed.
fn validate_single_common_listing(
    filing: &market_squawk_data::SecVerifiedFilingXbrl,
) -> Result<(), AutomaticValuationError> {
    let unavailable =
        AutomaticValuationError::Unavailable(AutomaticValuationUnavailable::MethodInput);
    let mut common_context = None;
    for occurrence in filing.nonnumeric_occurrences().iter() {
        let occurrence = occurrence.map_err(|_| unavailable)?;
        if !matches!(
            occurrence.concept().local_name().as_str(),
            "Security12bTitle" | "Security12gTitle"
        ) || !occurrence
            .concept()
            .namespace_uri()
            .is_some_and(|uri| uri.as_str().starts_with("http://xbrl.sec.gov/dei/"))
        {
            continue;
        }
        if occurrence.is_nil() {
            return Err(unavailable);
        }
        let context = filing
            .context(occurrence.context_id())
            .map_err(|_| unavailable)?
            .ok_or(unavailable)?;
        let title = occurrence.lexical_value().as_str().to_ascii_lowercase();
        let common = title.starts_with("common stock") || title.starts_with("ordinary shares");
        if common {
            let dimensions = context.dimensions();
            if !dimensions.is_empty() {
                let [dimension] = dimensions else {
                    return Err(unavailable);
                };
                let market_squawk_domain::XbrlDimensionMember::Explicit { member } =
                    dimension.member()
                else {
                    return Err(unavailable);
                };
                if dimension.dimension().local_name().as_str() != "StatementClassOfStockAxis"
                    || !dimension
                        .dimension()
                        .namespace_uri()
                        .is_some_and(|uri| uri.as_str().starts_with("http://fasb.org/us-gaap/"))
                    || member.local_name().as_str() != "CommonStockMember"
                    || member.namespace_uri() != dimension.dimension().namespace_uri()
                {
                    return Err(unavailable);
                }
            }
            if common_context
                .as_ref()
                .is_some_and(|prior| prior != context.context_id())
            {
                return Err(unavailable);
            }
            common_context = Some(context.context_id().clone());
        } else {
            // Admit only explicit rate-bearing notes with a stated due year. This narrow rule
            // supports additional registered debt without assuming unknown security rights.
            let words: Vec<_> = title.split_whitespace().collect();
            if words.len() != 4
                || !words[0].ends_with('%')
                || words[1] != "notes"
                || words[2] != "due"
                || words[3].len() != 4
                || !words[3].bytes().all(|c| c.is_ascii_digit())
                || words[0].trim_end_matches('%').parse::<Decimal>().is_err()
            {
                return Err(unavailable);
            }
        }
    }
    if common_context.is_none() {
        return Err(unavailable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::divide_common_shares;
    use market_squawk_data::ShareConversionRounding::{Central, Lower, Upper};
    use rust_decimal::Decimal;

    #[test]
    fn total_equity_division_preserves_outward_bounds_and_per_share_values() {
        // The authentic MSFT count is a numeric arithmetic example, not a source receipt.
        let shares = Decimal::from(7_433_166_379_u64);
        let total = shares.checked_mul(Decimal::new(12345, 2)).unwrap();
        assert_eq!(
            divide_common_shares(total, shares, 2, Central).unwrap(),
            Decimal::new(12345, 2)
        );
        let low = divide_common_shares(Decimal::from(100), Decimal::from(3), 2, Lower).unwrap();
        let high = divide_common_shares(Decimal::from(100), Decimal::from(3), 2, Upper).unwrap();
        assert_eq!((low, high), (Decimal::new(3333, 2), Decimal::new(3334, 2)));
        // Already-per-share comparable values have denominator one, with no second division.
        assert_eq!(
            divide_common_shares(Decimal::new(12345, 2), Decimal::ONE, 2, Central).unwrap(),
            Decimal::new(12345, 2)
        );
        assert_eq!(
            divide_common_shares(Decimal::from(1), Decimal::from(8), 2, Central).unwrap(),
            Decimal::new(12, 2)
        );
        assert_eq!(
            divide_common_shares(Decimal::from(3), Decimal::from(8), 2, Central).unwrap(),
            Decimal::new(38, 2)
        );
        assert!(divide_common_shares(total, Decimal::ZERO, 2, Central).is_err());
        assert!(divide_common_shares(total, Decimal::new(15, 1), 2, Central).is_err());
    }
}
