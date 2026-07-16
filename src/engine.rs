use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    bot::{MomentumBot, PaperAccount},
    domain::MarketEvent,
    features::OnlineFeatures,
    order_book::{OrderBook, TopOfBook},
    quality::{FeedQuality, QualityState},
    risk::{RiskDecision, RiskKernel, RiskLimits, RiskState},
};

pub type SharedEngine = Arc<RwLock<Engine>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductSnapshot {
    pub product: String,
    pub source: Option<String>,
    pub top: Option<TopOfBook>,
    pub features: Option<OnlineFeatures>,
    pub quality: FeedQuality,
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub last_update_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshot {
    pub source_status: BTreeMap<String, String>,
    pub products: BTreeMap<String, ProductSnapshot>,
    pub paper_account: PaperAccount,
    pub risk: RiskState,
    pub processed_events: u64,
}

#[derive(Debug, Default)]
struct ProductState {
    source: Option<String>,
    book: OrderBook,
    has_snapshot: bool,
    features: Option<OnlineFeatures>,
    quality: FeedQuality,
    last_update_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct Engine {
    products: HashMap<String, ProductState>,
    source_status: BTreeMap<String, String>,
    paper_account: PaperAccount,
    paper_bot: Option<MomentumBot>,
    risk: RiskKernel,
    processed_events: u64,
    stale_after_ms: i64,
}

impl Engine {
    #[must_use]
    pub fn new(stale_after_ms: i64, paper_bot_enabled: bool) -> Self {
        Self {
            products: HashMap::new(),
            source_status: BTreeMap::new(),
            paper_account: PaperAccount::default(),
            paper_bot: paper_bot_enabled.then(MomentumBot::default),
            risk: RiskKernel::new(RiskLimits {
                max_data_age_ms: stale_after_ms,
                ..RiskLimits::default()
            }),
            processed_events: 0,
            stale_after_ms,
        }
    }

    pub fn handle(&mut self, event: MarketEvent) {
        self.processed_events = self.processed_events.saturating_add(1);

        match event {
            MarketEvent::BookSnapshot {
                source,
                product,
                bids,
                asks,
                received_at,
            } => {
                let tradable = {
                    let state = self.products.entry(product.clone()).or_default();
                    state.source = Some(source);
                    state.book.apply_snapshot(&bids, &asks);
                    state.has_snapshot = true;
                    state.last_update_at = Some(received_at);
                    if state.book.is_crossed() {
                        state
                            .quality
                            .mark_quarantined("crossed order book after snapshot");
                        state.features = None;
                        false
                    } else {
                        state.quality.accept_snapshot(received_at);
                        state.features =
                            state.book.top().as_ref().and_then(OnlineFeatures::from_top);
                        state.features.is_some()
                    }
                };
                if tradable {
                    self.maybe_run_paper_bot(&product, received_at);
                }
            }
            MarketEvent::BookDelta {
                source,
                product,
                changes,
                received_at,
                ..
            } => {
                let tradable = {
                    let state = self.products.entry(product.clone()).or_default();
                    state.source = Some(source);
                    state.last_update_at = Some(received_at);
                    if !state.has_snapshot {
                        state
                            .quality
                            .mark_quarantined("received order-book delta before a snapshot");
                        state.features = None;
                        false
                    } else {
                        state.book.apply_changes(&changes);
                        if state.book.is_crossed() {
                            state
                                .quality
                                .mark_quarantined("crossed order book after delta");
                            state.features = None;
                            false
                        } else if state.quality.accept_delta(received_at) {
                            state.features =
                                state.book.top().as_ref().and_then(OnlineFeatures::from_top);
                            state.features.is_some()
                        } else {
                            state.features = None;
                            false
                        }
                    }
                };
                if tradable {
                    self.maybe_run_paper_bot(&product, received_at);
                }
            }
            MarketEvent::Heartbeat {
                source,
                product,
                sequence,
                received_at,
                ..
            } => {
                let state = self.products.entry(product).or_default();
                state.source = Some(source);
                state.quality.observe_heartbeat(received_at, sequence);
                if !state.quality.state.tradable() {
                    state.features = None;
                }
            }
            MarketEvent::Trade {
                source,
                product,
                received_at,
                ..
            } => {
                let state = self.products.entry(product).or_default();
                state.source = Some(source);
                state.last_update_at = Some(received_at);
            }
            MarketEvent::SourceStatus {
                source,
                status,
                detail,
                ..
            } => {
                let value =
                    detail.map_or_else(|| status.clone(), |detail| format!("{status}: {detail}"));
                self.source_status.insert(source.clone(), value);

                if matches!(status.as_str(), "connecting" | "disconnected" | "error") {
                    for state in self
                        .products
                        .values_mut()
                        .filter(|state| state.source.as_deref() == Some(source.as_str()))
                    {
                        state.has_snapshot = false;
                        state.features = None;
                        state.quality.mark_quarantined(format!(
                            "source {source} entered {status}; fresh snapshot required"
                        ));
                    }
                }
            }
        }
    }

    fn maybe_run_paper_bot(&mut self, product: &str, at: DateTime<Utc>) {
        let state = match self.products.get(product) {
            Some(state) => state,
            None => return,
        };
        let features = match state.features.clone() {
            Some(features) => features,
            None => return,
        };
        let quality = state.quality.clone();

        let bot = match &mut self.paper_bot {
            Some(bot) => bot,
            None => return,
        };
        let intent = match bot.on_features(product, &features, at) {
            Some(intent) => intent,
            None => return,
        };

        let position = self.paper_account.position(product);
        if matches!(
            self.risk.evaluate(&intent, &quality, position, at),
            RiskDecision::Approved
        ) {
            self.paper_account.fill(&intent);
        }
    }

    pub fn refresh_staleness(&mut self, now: DateTime<Utc>) {
        for state in self.products.values_mut() {
            state.quality.refresh_staleness(now, self.stale_after_ms);
            if state.quality.state == QualityState::Stale {
                state.features = None;
            }
        }
    }

    pub fn trigger_kill_switch(&mut self) {
        self.risk.trigger_kill_switch();
    }

    #[must_use]
    pub fn snapshot(&self) -> EngineSnapshot {
        let products = self
            .products
            .iter()
            .map(|(product, state)| {
                (
                    product.clone(),
                    ProductSnapshot {
                        product: product.clone(),
                        source: state.source.clone(),
                        top: state.book.top(),
                        features: state.features.clone(),
                        quality: state.quality.clone(),
                        bid_levels: state.book.bid_levels(),
                        ask_levels: state.book.ask_levels(),
                        last_update_at: state.last_update_at,
                    },
                )
            })
            .collect();

        EngineSnapshot {
            source_status: self.source_status.clone(),
            products,
            paper_account: self.paper_account.clone(),
            risk: self.risk.state.clone(),
            processed_events: self.processed_events,
        }
    }

    #[must_use]
    pub fn mid_price(&self, product: &str) -> Option<Decimal> {
        self.products
            .get(product)
            .and_then(|state| state.features.as_ref())
            .map(|features| features.mid_price)
    }
}
