use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{bot::OrderIntent, quality::FeedQuality};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_absolute_position: Decimal,
    pub max_data_age_ms: i64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_notional: Decimal::from(1_000_u32),
            max_absolute_position: Decimal::from(1_u32),
            max_data_age_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RiskDecision {
    Approved,
    Rejected { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RiskState {
    pub kill_switch: bool,
    pub rejected_orders: u64,
    pub approved_orders: u64,
}

#[derive(Debug, Clone)]
pub struct RiskKernel {
    pub limits: RiskLimits,
    pub state: RiskState,
}

impl RiskKernel {
    #[must_use]
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            state: RiskState::default(),
        }
    }

    pub fn evaluate(
        &mut self,
        intent: &OrderIntent,
        quality: &FeedQuality,
        current_position: Decimal,
        now: DateTime<Utc>,
    ) -> RiskDecision {
        let rejection = if self.state.kill_switch {
            Some("kill switch is active".to_owned())
        } else if !quality.state.tradable() {
            Some(format!("market data is not tradable: {:?}", quality.state))
        } else if quality.last_book_at.is_none_or(|last| {
            now.signed_duration_since(last).num_milliseconds() > self.limits.max_data_age_ms
        }) {
            Some("market data is stale".to_owned())
        } else if intent.quantity <= Decimal::ZERO || intent.limit_price <= Decimal::ZERO {
            Some("quantity and price must be positive".to_owned())
        } else if intent.quantity * intent.limit_price > self.limits.max_order_notional {
            Some("order notional exceeds configured limit".to_owned())
        } else if (current_position + intent.signed_quantity()).abs()
            > self.limits.max_absolute_position
        {
            Some("resulting position exceeds configured limit".to_owned())
        } else {
            None
        };

        if let Some(reason) = rejection {
            self.state.rejected_orders = self.state.rejected_orders.saturating_add(1);
            RiskDecision::Rejected { reason }
        } else {
            self.state.approved_orders = self.state.approved_orders.saturating_add(1);
            RiskDecision::Approved
        }
    }

    pub fn trigger_kill_switch(&mut self) {
        self.state.kill_switch = true;
    }
}
