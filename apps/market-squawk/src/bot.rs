use std::collections::{BTreeMap, HashMap, VecDeque};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{domain::Side, features::OnlineFeatures};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub intent_id: Uuid,
    pub strategy: String,
    pub product: String,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub limit_price: Decimal,
    pub created_at: DateTime<Utc>,
    pub reason: String,
}

impl OrderIntent {
    #[must_use]
    pub fn signed_quantity(&self) -> Decimal {
        match self.side {
            Side::Buy => self.quantity,
            Side::Sell => -self.quantity,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperFill {
    pub fill_id: Uuid,
    pub intent_id: Uuid,
    pub product: String,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub filled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PaperAccount {
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_cash_flow: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fees: Decimal,
    pub positions: BTreeMap<String, Decimal>,
    pub fills: Vec<PaperFill>,
}

impl PaperAccount {
    pub fn fill(&mut self, intent: &OrderIntent) -> PaperFill {
        let signed_quantity = intent.signed_quantity();
        *self.positions.entry(intent.product.clone()).or_default() += signed_quantity;
        self.realized_cash_flow -= signed_quantity * intent.limit_price;

        let fill = PaperFill {
            fill_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, intent.intent_id.as_bytes()),
            intent_id: intent.intent_id,
            product: intent.product.clone(),
            side: intent.side,
            quantity: intent.quantity,
            price: intent.limit_price,
            filled_at: intent.created_at,
        };
        self.fills.push(fill.clone());
        fill
    }

    #[must_use]
    pub fn position(&self, product: &str) -> Decimal {
        self.positions.get(product).copied().unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct MomentumBot {
    lookback: usize,
    threshold_bps: Decimal,
    order_quantity: Decimal,
    history: HashMap<String, VecDeque<Decimal>>,
}

impl Default for MomentumBot {
    fn default() -> Self {
        Self {
            lookback: 20,
            threshold_bps: Decimal::from(5_u32),
            order_quantity: Decimal::new(1, 3),
            history: HashMap::new(),
        }
    }
}

impl MomentumBot {
    #[must_use]
    pub fn on_features(
        &mut self,
        product: &str,
        features: &OnlineFeatures,
        at: DateTime<Utc>,
    ) -> Option<OrderIntent> {
        let history = self.history.entry(product.to_owned()).or_default();
        history.push_back(features.mid_price);

        while history.len() > self.lookback {
            history.pop_front();
        }

        if history.len() < self.lookback {
            return None;
        }

        let first = *history.front()?;
        if first == Decimal::ZERO {
            return None;
        }

        let move_bps = (features.mid_price - first) / first * Decimal::from(10_000_u32);
        let side = if move_bps >= self.threshold_bps {
            Side::Buy
        } else if move_bps <= -self.threshold_bps {
            Side::Sell
        } else {
            return None;
        };

        let side_name = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };
        let identity = format!(
            "paper-momentum-v1:{product}:{}:{}:{side_name}",
            at.to_rfc3339(),
            features.mid_price
        );

        Some(OrderIntent {
            intent_id: Uuid::new_v5(&Uuid::NAMESPACE_URL, identity.as_bytes()),
            strategy: "paper-momentum-v1".to_owned(),
            product: product.to_owned(),
            side,
            quantity: self.order_quantity,
            limit_price: features.mid_price,
            created_at: at,
            reason: format!("{move_bps} bps move over {} observations", self.lookback),
        })
    }
}
