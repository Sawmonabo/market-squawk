use std::num::NonZeroU64;

use market_squawk_domain::{
    AccountId, ConnectionGeneration, DataQuality, InstrumentExecutionTerms, InstrumentId,
    LiveEventClass, Money, PriceTicks, QuantityLots, Timestamp,
};
use market_squawk_execution::ExecutionMarketUpdate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::validate_terms;
use super::{PaperAccountRiskState, PaperLedger, PaperLedgerError, checked_notional};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PaperMarkInput {
    pub(super) terms: InstrumentExecutionTerms,
    pub(super) venue_digest: [u8; 32],
    pub(super) connection_generation: ConnectionGeneration,
    pub(super) quality: DataQuality,
    pub(super) event_class: LiveEventClass,
    pub(super) assessment_digest: [u8; 32],
    pub(super) observed_at: Timestamp,
    pub(super) best_bid: PriceTicks,
    pub(super) best_ask: PriceTicks,
}

/// Exact current executable-exit evidence retained independently of order matching state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PaperMarkEvidence {
    terms: InstrumentExecutionTerms,
    venue_digest: [u8; 32],
    connection_generation: ConnectionGeneration,
    quality: DataQuality,
    event_class: LiveEventClass,
    assessment_digest: [u8; 32],
    observed_at: Timestamp,
    best_bid: PriceTicks,
    best_ask: PriceTicks,
    digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PaperMarkDisposition {
    Applied,
    Irrelevant,
}

impl PaperMarkEvidence {
    pub(super) fn try_new(input: PaperMarkInput) -> Result<Self, PaperLedgerError> {
        if input.quality != DataQuality::DirectVerified
            || !matches!(
                input.event_class,
                LiveEventClass::Trade
                    | LiveEventClass::Quote
                    | LiveEventClass::BookSnapshot
                    | LiveEventClass::BookDelta
            )
            || input.venue_digest == [0; 32]
            || input.assessment_digest == [0; 32]
            || input.best_bid.get() <= 0
            || input.best_ask.get() <= 0
            || input.best_bid >= input.best_ask
        {
            return Err(PaperLedgerError::InvalidMark);
        }
        let digest = mark_digest(input);
        Ok(Self {
            terms: input.terms,
            venue_digest: input.venue_digest,
            connection_generation: input.connection_generation,
            quality: input.quality,
            event_class: input.event_class,
            assessment_digest: input.assessment_digest,
            observed_at: input.observed_at,
            best_bid: input.best_bid,
            best_ask: input.best_ask,
            digest,
        })
    }

    pub(super) const fn instrument_id(self) -> InstrumentId {
        self.terms.instrument_id()
    }

    pub(super) fn try_from_update(update: ExecutionMarketUpdate) -> Result<Self, PaperLedgerError> {
        let market = update.market();
        let best_bid = market
            .best_bid()
            .ok_or(PaperLedgerError::InvalidMark)?
            .price();
        let best_ask = market
            .best_ask()
            .ok_or(PaperLedgerError::InvalidMark)?
            .price();
        Self::try_new(PaperMarkInput {
            terms: market.execution_terms(),
            venue_digest: update.venue_digest(),
            connection_generation: update.connection_generation(),
            quality: market.quality(),
            event_class: update.event_class(),
            assessment_digest: update.assessment_digest(),
            observed_at: market.observed_at(),
            best_bid,
            best_ask,
        })
    }

    pub(super) const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    pub(super) fn validate_recovered(self) -> Result<(), PaperLedgerError> {
        let rebuilt = Self::try_new(PaperMarkInput {
            terms: self.terms,
            venue_digest: self.venue_digest,
            connection_generation: self.connection_generation,
            quality: self.quality,
            event_class: self.event_class,
            assessment_digest: self.assessment_digest,
            observed_at: self.observed_at,
            best_bid: self.best_bid,
            best_ask: self.best_ask,
        })?;
        if rebuilt != self {
            return Err(PaperLedgerError::InvalidRecovery);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MarkedAccountImage {
    marked_equity: Money,
    peak_marked_equity: Money,
    marked_gross_exposure: Money,
    unrealized_pnl: Money,
    drawdown: Money,
    mark_digest: [u8; 32],
}

impl PaperLedger {
    pub(super) fn apply_mark(
        &mut self,
        mark: PaperMarkEvidence,
        valued_at: Timestamp,
        maximum_mark_age_nanos: u64,
    ) -> Result<PaperMarkDisposition, PaperLedgerError> {
        validate_terms(mark.terms, self.config.fee_schedule.currency())?;
        validate_fresh(mark, valued_at, maximum_mark_age_nanos)?;
        let instrument_id = mark.instrument_id();
        let relevant = self
            .positions
            .keys()
            .any(|(_, candidate)| *candidate == instrument_id)
            || self
                .reservations
                .values()
                .any(|reservation| reservation.terms.instrument_id() == instrument_id);
        if !relevant {
            return Ok(PaperMarkDisposition::Irrelevant);
        }
        if !self.marks.contains_key(&instrument_id)
            && self.marks.len() >= self.config.maximum_positions
        {
            return Err(PaperLedgerError::Capacity);
        }
        if let Some(current) = self.marks.get(&instrument_id).copied() {
            if current.terms != mark.terms || current.venue_digest != mark.venue_digest {
                return Err(PaperLedgerError::InvalidMark);
            }
            if mark.connection_generation < current.connection_generation
                || mark.observed_at < current.observed_at
            {
                return Err(PaperLedgerError::MarkRegression);
            }
        }

        let mut replacements = Vec::new();
        for (account_id, account) in &self.accounts {
            if !self.positions.contains_key(&(*account_id, instrument_id)) {
                continue;
            }
            let revision = account
                .revision
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or(PaperLedgerError::Overflow)?;
            let image = self.calculate_account_marks(
                *account_id,
                *account,
                valued_at,
                maximum_mark_age_nanos,
                Some(mark),
            )?;
            let next = account_with_image(*account, revision, image)?;
            replacements.push((*account_id, next));
        }

        self.marks.insert(instrument_id, mark);
        for (account_id, account) in replacements {
            self.accounts.insert(account_id, account);
        }
        Ok(PaperMarkDisposition::Applied)
    }

    pub(super) fn compact_unused_marks(&mut self) {
        let positions = &self.positions;
        let reservations = &self.reservations;
        self.marks.retain(|instrument_id, _| {
            positions
                .keys()
                .any(|(_, candidate)| candidate == instrument_id)
                || reservations
                    .values()
                    .any(|reservation| reservation.terms.instrument_id() == *instrument_id)
        });
    }

    pub(super) fn validate_account_marks(
        &self,
        account_id: AccountId,
        account: PaperAccountRiskState,
        valued_at: Timestamp,
        maximum_mark_age_nanos: u64,
    ) -> Result<(), PaperLedgerError> {
        let image = self
            .calculate_account_marks(account_id, account, valued_at, maximum_mark_age_nanos, None)?
            .ok_or(PaperLedgerError::StaleMark)?;
        if account.marked_equity != image.marked_equity
            || account.peak_marked_equity != image.peak_marked_equity
            || account.marked_gross_exposure != image.marked_gross_exposure
            || account.unrealized_pnl != image.unrealized_pnl
            || account.drawdown != image.drawdown
            || account.mark_digest != image.mark_digest
        {
            return Err(PaperLedgerError::InvalidRecovery);
        }
        Ok(())
    }

    fn calculate_account_marks(
        &self,
        account_id: AccountId,
        account: PaperAccountRiskState,
        valued_at: Timestamp,
        maximum_mark_age_nanos: u64,
        mark_override: Option<PaperMarkEvidence>,
    ) -> Result<Option<MarkedAccountImage>, PaperLedgerError> {
        let zero = Money::new(rust_decimal::Decimal::ZERO, account.currency);
        let mut exposure = zero;
        let mut unrealized = zero;
        let mut aggregate = Sha256::new();
        aggregate.update(b"market-squawk/paper-account-marks/v1\0");
        aggregate.update(account_id.as_uuid().as_bytes());
        let mut position_count = 0_u64;
        for ((candidate, instrument_id), lots) in &self.positions {
            if *candidate != account_id {
                continue;
            }
            position_count = position_count
                .checked_add(1)
                .ok_or(PaperLedgerError::Overflow)?;
            let mark = mark_override
                .filter(|candidate_mark| candidate_mark.instrument_id() == *instrument_id)
                .or_else(|| self.marks.get(instrument_id).copied());
            let Some(mark) = mark else {
                return Ok(None);
            };
            if mark.terms.instrument_id() != *instrument_id
                || mark.terms.quote_currency() != account.currency
            {
                return Err(PaperLedgerError::InvalidMark);
            }
            if validate_fresh(mark, valued_at, maximum_mark_age_nanos).is_err() {
                return Ok(None);
            }
            let absolute_lots =
                i64::try_from(lots.unsigned_abs()).map_err(|_| PaperLedgerError::Overflow)?;
            let quantity = QuantityLots::new(absolute_lots)
                .map_err(|_| PaperLedgerError::InvalidQuantityOrPrice)?;
            let exit_price = if *lots > 0 {
                mark.best_bid
            } else {
                mark.best_ask
            };
            let marked_notional = checked_notional(mark.terms, exit_price, quantity)?;
            let cost_basis = Money::new(
                self.position_cost_basis
                    .get(&(account_id, *instrument_id))
                    .copied()
                    .ok_or(PaperLedgerError::InvalidRecovery)?,
                account.currency,
            );
            let position_pnl = if *lots > 0 {
                marked_notional.checked_sub(cost_basis)
            } else {
                cost_basis.checked_sub(marked_notional)
            }
            .map_err(|_| PaperLedgerError::Overflow)?;
            exposure = exposure
                .checked_add(marked_notional)
                .map_err(|_| PaperLedgerError::Overflow)?;
            unrealized = unrealized
                .checked_add(position_pnl)
                .map_err(|_| PaperLedgerError::Overflow)?;
            aggregate.update(instrument_id.as_uuid().as_bytes());
            aggregate.update(lots.to_be_bytes());
            aggregate.update(mark.digest);
        }
        let mark_digest = if position_count == 0 {
            [0; 32]
        } else {
            aggregate.finalize().into()
        };
        let marked_equity = account
            .settled_capital
            .checked_add(unrealized)
            .map_err(|_| PaperLedgerError::Overflow)?;
        let peak_marked_equity = if marked_equity.amount() > account.peak_marked_equity.amount() {
            marked_equity
        } else {
            account.peak_marked_equity
        };
        let drawdown = peak_marked_equity
            .checked_sub(marked_equity)
            .map_err(|_| PaperLedgerError::Overflow)?;
        Ok(Some(MarkedAccountImage {
            marked_equity,
            peak_marked_equity,
            marked_gross_exposure: exposure,
            unrealized_pnl: unrealized,
            drawdown,
            mark_digest,
        }))
    }
}

fn account_with_image(
    account: PaperAccountRiskState,
    revision: NonZeroU64,
    image: Option<MarkedAccountImage>,
) -> Result<PaperAccountRiskState, PaperLedgerError> {
    let Some(image) = image else {
        let peak = if account.settled_capital.amount() > account.peak_marked_equity.amount() {
            account.settled_capital
        } else {
            account.peak_marked_equity
        };
        return Ok(PaperAccountRiskState {
            revision,
            marked_equity: account.settled_capital,
            peak_marked_equity: peak,
            marked_gross_exposure: Money::new(rust_decimal::Decimal::ZERO, account.currency),
            unrealized_pnl: Money::new(rust_decimal::Decimal::ZERO, account.currency),
            drawdown: peak
                .checked_sub(account.settled_capital)
                .map_err(|_| PaperLedgerError::Overflow)?,
            mark_digest: [0; 32],
            ..account
        });
    };
    Ok(PaperAccountRiskState {
        revision,
        marked_equity: image.marked_equity,
        peak_marked_equity: image.peak_marked_equity,
        marked_gross_exposure: image.marked_gross_exposure,
        unrealized_pnl: image.unrealized_pnl,
        drawdown: image.drawdown,
        mark_digest: image.mark_digest,
        ..account
    })
}

fn validate_fresh(
    mark: PaperMarkEvidence,
    valued_at: Timestamp,
    maximum_mark_age_nanos: u64,
) -> Result<(), PaperLedgerError> {
    if maximum_mark_age_nanos == 0 {
        return Err(PaperLedgerError::InvalidConfiguration);
    }
    let age = i128::from(valued_at.unix_nanos()) - i128::from(mark.observed_at.unix_nanos());
    if age < 0 || age >= i128::from(maximum_mark_age_nanos) {
        return Err(PaperLedgerError::StaleMark);
    }
    Ok(())
}

fn mark_digest(input: PaperMarkInput) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/paper-executable-mark/v1\0");
    digest.update(input.terms.instrument_id().as_uuid().as_bytes());
    digest.update(input.terms.definition_revision().get().to_be_bytes());
    digest_decimal(&mut digest, input.terms.price_tick().as_decimal());
    digest_decimal(&mut digest, input.terms.lot_size().as_decimal());
    digest.update(input.terms.quote_currency().as_str().as_bytes());
    digest_decimal(&mut digest, input.terms.contract_multiplier());
    digest.update(input.venue_digest);
    digest.update(input.connection_generation.get().to_be_bytes());
    digest.update([event_class_tag(input.event_class)]);
    digest.update(input.assessment_digest);
    digest.update(input.observed_at.unix_nanos().to_be_bytes());
    digest.update(input.best_bid.get().to_be_bytes());
    digest.update(input.best_ask.get().to_be_bytes());
    digest.finalize().into()
}

fn digest_decimal(digest: &mut Sha256, value: rust_decimal::Decimal) {
    digest.update(value.mantissa().to_be_bytes());
    digest.update(value.scale().to_be_bytes());
}

const fn event_class_tag(event_class: LiveEventClass) -> u8 {
    match event_class {
        LiveEventClass::Trade => 0,
        LiveEventClass::Quote => 1,
        LiveEventClass::BookSnapshot => 2,
        LiveEventClass::BookDelta => 3,
        LiveEventClass::Auction => 4,
        LiveEventClass::TradingHalt => 5,
        LiveEventClass::InstrumentStatus => 6,
        LiveEventClass::CorporateAction => 7,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::str::FromStr;

    use market_squawk_domain::{
        AccountId, ConnectionGeneration, Currency, DataQuality, Denomination,
        InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LiveEventClass,
        LotSize, Money, OrderId, OrderSide, PriceTicks, QuantityLots, TickSize, Timestamp,
    };
    use rust_decimal::Decimal;

    use super::{PaperMarkDisposition, PaperMarkEvidence, PaperMarkInput};
    use crate::{
        FeeSchedule, LiquidityRole, PaperAccountBootstrap, PaperExposureValuation, PaperLedger,
        PaperLedgerConfig,
    };

    #[test]
    fn executable_marks_revalue_exactly_and_survive_recovery_without_regression()
    -> Result<(), Box<dyn std::error::Error>> {
        let usd = Currency::try_from("USD")?;
        let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000031")?;
        let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000031")?;
        let terms = InstrumentExecutionTerms::try_new(
            instrument_id,
            InstrumentDefinitionRevision::try_from(1)?,
            TickSize::try_from_decimal(Decimal::ONE)?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            usd,
            Denomination::Currency(usd),
            Decimal::ONE,
        )?;
        let config = PaperLedgerConfig {
            allow_short: false,
            exposure_valuation: PaperExposureValuation::ExecutableExit,
            maximum_accounts: 1,
            maximum_balances: 1,
            maximum_positions: 1,
            maximum_reservations: 1,
            fee_schedule: FeeSchedule::try_new(0, 0, Money::new(Decimal::ZERO, usd), None, 2)?,
        };
        let mut ledger = PaperLedger::try_new(
            config,
            [PaperAccountBootstrap {
                account_id,
                revision: NonZeroU64::MIN,
                eligible: true,
                cash: vec![Money::new(Decimal::new(1_000, 0), usd)],
                capital: Money::new(Decimal::new(1_000, 0), usd),
                peak_capital: Money::new(Decimal::new(1_000, 0), usd),
                gross_exposure: Money::new(Decimal::ZERO, usd),
                realized_pnl: Money::new(Decimal::ZERO, usd),
                realized_loss: Money::new(Decimal::ZERO, usd),
                positions: Vec::new(),
                position_cost_basis: Vec::new(),
            }],
        )?;
        let order_id = OrderId::from_str("20000000-0000-0000-0000-000000000031")?;
        ledger.reserve(
            order_id,
            account_id,
            terms,
            OrderSide::Buy,
            QuantityLots::new(1)?,
            PriceTicks::new(100),
        )?;
        ledger.apply_fill(
            order_id,
            terms,
            &[(PriceTicks::new(100), QuantityLots::new(1)?)],
            LiquidityRole::Taker,
        )?;

        let valued_at = Timestamp::from_unix_nanos(1_000);
        let unrelated_terms = InstrumentExecutionTerms::try_new(
            InstrumentId::from_str("10000000-0000-0000-0000-000000000032")?,
            InstrumentDefinitionRevision::try_from(1)?,
            TickSize::try_from_decimal(Decimal::ONE)?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            usd,
            Denomination::Currency(usd),
            Decimal::ONE,
        )?;
        let before_irrelevant = ledger.account_risk(account_id)?;
        assert_eq!(
            ledger.apply_mark(
                PaperMarkEvidence::try_new(PaperMarkInput {
                    terms: unrelated_terms,
                    venue_digest: [6; 32],
                    connection_generation: ConnectionGeneration::new(1)?,
                    quality: DataQuality::DirectVerified,
                    event_class: LiveEventClass::Quote,
                    assessment_digest: [5; 32],
                    observed_at: Timestamp::from_unix_nanos(999),
                    best_bid: PriceTicks::new(20),
                    best_ask: PriceTicks::new(21),
                })?,
                valued_at,
                10,
            )?,
            PaperMarkDisposition::Irrelevant
        );
        assert_eq!(ledger.account_risk(account_id)?, before_irrelevant);

        let mark = PaperMarkEvidence::try_new(PaperMarkInput {
            terms,
            venue_digest: [7; 32],
            connection_generation: ConnectionGeneration::new(2)?,
            quality: DataQuality::DirectVerified,
            event_class: LiveEventClass::BookDelta,
            assessment_digest: [8; 32],
            observed_at: Timestamp::from_unix_nanos(999),
            best_bid: PriceTicks::new(40),
            best_ask: PriceTicks::new(41),
        })?;
        assert_eq!(
            ledger.apply_mark(mark, valued_at, 10)?,
            PaperMarkDisposition::Applied
        );
        let marked = ledger.account_risk(account_id)?;
        assert_eq!(marked.settled_capital().amount(), Decimal::new(1_000, 0));
        assert_eq!(marked.marked_equity().amount(), Decimal::new(940, 0));
        assert_eq!(marked.unrealized_pnl().amount(), Decimal::new(-60, 0));
        assert_eq!(marked.marked_gross_exposure().amount(), Decimal::new(40, 0));
        assert_eq!(marked.drawdown().amount(), Decimal::new(60, 0));
        assert_ne!(marked.mark_digest(), [0; 32]);

        let before = marked;
        let stale_generation = PaperMarkEvidence::try_new(PaperMarkInput {
            terms,
            venue_digest: [7; 32],
            connection_generation: ConnectionGeneration::new(1)?,
            quality: DataQuality::DirectVerified,
            event_class: LiveEventClass::BookDelta,
            assessment_digest: [9; 32],
            observed_at: Timestamp::from_unix_nanos(1_000),
            best_bid: PriceTicks::new(39),
            best_ask: PriceTicks::new(40),
        })?;
        assert!(ledger.apply_mark(stale_generation, valued_at, 10).is_err());
        assert_eq!(ledger.account_risk(account_id)?, before);

        let recovered = PaperLedger::try_from_recovery_wire(config, ledger.recovery_wire())?;
        assert_eq!(recovered.account_risk(account_id)?, before);
        Ok(())
    }
}
