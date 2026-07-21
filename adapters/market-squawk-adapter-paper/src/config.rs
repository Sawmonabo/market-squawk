//! Frozen bounded paper-simulation configuration.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use market_squawk_domain::Currency;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{FeeSchedule, PaperExposureValuation, PaperLedgerConfig, PaperVenueSessionCalendar};

/// Construction input for a realistic paper worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperExecutionConfigInput {
    pub configuration_version: NonZeroU64,
    pub deterministic_seed: [u8; 32],
    pub command_capacity: NonZeroUsize,
    pub command_maximum_bytes: NonZeroU32,
    pub market_capacity: NonZeroUsize,
    pub market_maximum_bytes: NonZeroU32,
    pub audit_capacity: NonZeroUsize,
    pub audit_maximum_bytes: NonZeroU32,
    pub maximum_orders: NonZeroUsize,
    pub maximum_fills: NonZeroUsize,
    pub maximum_idempotency_keys: NonZeroUsize,
    pub maximum_archived_orders: NonZeroUsize,
    pub minimum_latency_nanos: u64,
    pub maximum_latency_nanos: u64,
    pub cancel_latency_nanos: u64,
    pub day_session_calendar: PaperVenueSessionCalendar,
    pub maximum_participation_basis_points: u32,
    pub impact_basis_points_per_level: u32,
    pub reporting_currency: Currency,
    pub ledger_maximum_accounts: NonZeroUsize,
    pub ledger_maximum_balances: NonZeroUsize,
    pub ledger_maximum_positions: NonZeroUsize,
    pub allow_short: bool,
    pub exposure_valuation: PaperExposureValuation,
    pub abort_join_deadline: Duration,
    pub fee_schedule: FeeSchedule,
}

/// Validated immutable simulation configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperExecutionConfig {
    input: PaperExecutionConfigInput,
    digest: [u8; 32],
}

impl PaperExecutionConfig {
    pub const CHECKPOINT_SCHEMA_VERSION: u32 = 4;

    /// Validates bounds and seals a stable configuration digest.
    pub fn try_new(input: PaperExecutionConfigInput) -> Result<Self, PaperConfigError> {
        if input.minimum_latency_nanos > input.maximum_latency_nanos
            || input.maximum_latency_nanos > i64::MAX as u64
            || input.cancel_latency_nanos > i64::MAX as u64
            || input.abort_join_deadline.is_zero()
            || !(1..=10_000).contains(&input.maximum_participation_basis_points)
            || input.impact_basis_points_per_level > 10_000
            || input.fee_schedule.currency() != input.reporting_currency
        {
            return Err(PaperConfigError::InvalidValue);
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/paper-config/v3\0");
        digest.update(input.configuration_version.get().to_be_bytes());
        digest.update(input.deterministic_seed);
        let count_values = [
            input.command_capacity.get(),
            input.market_capacity.get(),
            input.audit_capacity.get(),
            input.maximum_orders.get(),
            input.maximum_fills.get(),
            input.maximum_idempotency_keys.get(),
            input.maximum_archived_orders.get(),
            input.ledger_maximum_accounts.get(),
            input.ledger_maximum_balances.get(),
            input.ledger_maximum_positions.get(),
        ];
        for value in count_values {
            digest.update(
                u64::try_from(value)
                    .map_err(|_| PaperConfigError::InvalidValue)?
                    .to_be_bytes(),
            );
        }
        for value in [
            u64::from(input.command_maximum_bytes.get()),
            u64::from(input.market_maximum_bytes.get()),
            u64::from(input.audit_maximum_bytes.get()),
            input.minimum_latency_nanos,
            input.maximum_latency_nanos,
            input.cancel_latency_nanos,
            u64::from(input.maximum_participation_basis_points),
            u64::from(input.impact_basis_points_per_level),
        ] {
            digest.update(value.to_be_bytes());
        }
        digest.update(input.day_session_calendar.digest());
        digest.update(input.reporting_currency.as_str().as_bytes());
        digest.update(input.abort_join_deadline.as_secs().to_be_bytes());
        digest.update(input.abort_join_deadline.subsec_nanos().to_be_bytes());
        digest.update([u8::from(input.allow_short)]);
        match input.exposure_valuation {
            PaperExposureValuation::OpenCost => digest.update(b"open-cost"),
        }
        digest.update(input.fee_schedule.maker_basis_points().to_be_bytes());
        digest.update(input.fee_schedule.taker_basis_points().to_be_bytes());
        digest.update(input.fee_schedule.money_scale().to_be_bytes());
        hash_money(&mut digest, input.fee_schedule.minimum_fee());
        match input.fee_schedule.maximum_fee() {
            Some(maximum) => {
                digest.update([1]);
                hash_money(&mut digest, maximum);
            }
            None => digest.update([0]),
        }
        Ok(Self {
            input,
            digest: digest.finalize().into(),
        })
    }

    pub const fn input(&self) -> &PaperExecutionConfigInput {
        &self.input
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub(crate) fn ledger_config(&self) -> PaperLedgerConfig {
        PaperLedgerConfig {
            allow_short: self.input.allow_short,
            exposure_valuation: self.input.exposure_valuation,
            maximum_accounts: self.input.ledger_maximum_accounts.get(),
            maximum_balances: self.input.ledger_maximum_balances.get(),
            maximum_positions: self.input.ledger_maximum_positions.get(),
            maximum_reservations: self.input.maximum_orders.get(),
            fee_schedule: self.input.fee_schedule,
        }
    }
}

fn hash_money(digest: &mut Sha256, money: market_squawk_domain::Money) {
    digest.update(money.currency().as_str().as_bytes());
    digest.update(money.amount().mantissa().to_be_bytes());
    digest.update(money.amount().scale().to_be_bytes());
}

/// Invalid frozen simulation configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PaperConfigError {
    #[error("paper execution configuration contains an invalid value")]
    InvalidValue,
}
