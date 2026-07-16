/// Typed, message-atomic provider payload variants.
#[derive(Clone, Debug)]
pub enum ProviderObservationPayload {
    /// Executed trade.
    Trade {
        /// Exact provider trade identity.
        trade_id: SourceIdentifier,
        /// Exact provider price.
        price: ProviderPrice,
        /// Exact provider quantity.
        quantity: ProviderQuantity,
        /// Exact typed aggressor/maker evidence.
        aggressor: ProviderAggressorEvidence,
    },
    /// Quote with at least one side; each side is price/quantity atomic.
    Quote {
        /// Optional bid side.
        bid: Option<ProviderBookLevel>,
        /// Optional ask side.
        ask: Option<ProviderBookLevel>,
    },
    /// Complete provider book image.
    BookSnapshot(ProviderBookSnapshotPayload),
    /// Incremental provider book change set.
    BookDelta(ProviderBookDeltaPayload),
    /// Auction indication/result with typed phase and required paired quantity.
    Auction {
        /// Exact provider auction code.
        provider_code: SourceIdentifier,
        /// Exact provider-code interpretation rule.
        rule: IntegrityRule,
        /// Typed auction phase.
        phase: AuctionPhase,
        /// Optional indicative/clearing price.
        price: Option<ProviderPrice>,
        /// Required paired/imbalance quantity.
        paired_quantity: ProviderQuantity,
    },
    /// Trading halt/resumption status.
    TradingHalt {
        status: ProviderStatusEvidence,
        transition: HaltTransition,
        /// Exact source-supplied halt/resumption reason required by canonicalization.
        reason: SourceIdentifier,
    },
    /// Instrument status transition.
    InstrumentStatus {
        status: ProviderStatusEvidence,
        trading_status: TradingStatus,
    },
    /// Corporate-action announcement identity and interpretation rule.
    CorporateAction {
        /// Exact provider action identity.
        action_id: SourceIdentifier,
        /// Exact action interpretation rule.
        rule: IntegrityRule,
        /// Economic effective time.
        effective_at: Timestamp,
        /// Typed corporate action kind.
        kind: CorporateActionKind,
    },
}

impl ProviderObservationPayload {
    /// Constructs a quote and rejects a missing two-sided payload.
    ///
    /// # Errors
    ///
    /// Rejects a quote with neither bid nor ask.
    pub fn quote(
        bid: Option<ProviderBookLevel>,
        ask: Option<ProviderBookLevel>,
    ) -> Result<Self, DecodeError> {
        if bid.is_none() && ask.is_none() {
            Err(DecodeError::InvalidProviderEvidence)
        } else {
            Ok(Self::Quote { bid, ask })
        }
    }

    /// Constructs a bounded provider snapshot.
    ///
    /// # Errors
    ///
    /// Rejects aggregate levels beyond the frame-level bound.
    pub fn book_snapshot(
        depth: MarketDepth,
        bids: Vec<ProviderBookLevel>,
        asks: Vec<ProviderBookLevel>,
    ) -> Result<Self, DecodeError> {
        if bids.len().saturating_add(asks.len()) > MAX_DECODED_BOOK_ITEMS {
            return Err(DecodeError::TooManyNumericFields {
                max: MAX_DECODED_BOOK_ITEMS,
            });
        }
        Ok(Self::BookSnapshot(ProviderBookSnapshotPayload {
            depth,
            bids: BoundedVec::try_new(bids)
                .map_err(|error| DecodeError::TooManyNumericFields { max: error.max })?,
            asks: BoundedVec::try_new(asks)
                .map_err(|error| DecodeError::TooManyNumericFields { max: error.max })?,
        }))
    }

    /// Constructs a nonempty bounded provider delta.
    ///
    /// # Errors
    ///
    /// Rejects empty or excessive change sets.
    pub fn book_delta(
        depth: MarketDepth,
        changes: Vec<ProviderBookChange>,
    ) -> Result<Self, DecodeError> {
        if changes.is_empty() {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        Ok(Self::BookDelta(ProviderBookDeltaPayload {
            depth,
            changes: BoundedVec::try_new(changes)
                .map_err(|error| DecodeError::TooManyNumericFields { max: error.max })?,
        }))
    }

    /// Returns canonical class identity without constructing a canonical event.
    pub const fn event_class(&self) -> LiveEventClass {
        match self {
            Self::Trade { .. } => LiveEventClass::Trade,
            Self::Quote { .. } => LiveEventClass::Quote,
            Self::BookSnapshot(_) => LiveEventClass::BookSnapshot,
            Self::BookDelta(_) => LiveEventClass::BookDelta,
            Self::Auction { .. } => LiveEventClass::Auction,
            Self::TradingHalt { .. } => LiveEventClass::TradingHalt,
            Self::InstrumentStatus { .. } => LiveEventClass::InstrumentStatus,
            Self::CorporateAction { .. } => LiveEventClass::CorporateAction,
        }
    }

    /// Returns market depth only for book payloads.
    pub const fn depth(&self) -> Option<MarketDepth> {
        match self {
            Self::BookSnapshot(value) => Some(value.depth),
            Self::BookDelta(value) => Some(value.depth),
            _ => None,
        }
    }

    fn book_item_count(&self) -> usize {
        match self {
            Self::BookSnapshot(value) => value.bids.len().saturating_add(value.asks.len()),
            Self::BookDelta(value) => value.changes.len(),
            _ => 0,
        }
    }

    fn deep_retained_bytes(&self) -> Result<usize, DecodeError> {
        let bytes = match self {
            Self::Trade {
                trade_id,
                price,
                quantity,
                aggressor,
            } => checked_sum([
                trade_id.as_str().len(),
                price.0.retained_bytes(),
                quantity.0.retained_bytes(),
                aggressor
                    .provider_code
                    .as_ref()
                    .map_or(0, |value| value.as_str().len()),
                aggressor.rule.provider_rule().as_str().len(),
            ])?,
            Self::Quote { bid, ask } => {
                checked_sum(bid.iter().chain(ask.iter()).flat_map(|level| {
                    [
                        level.price.0.retained_bytes(),
                        level.quantity.0.retained_bytes(),
                    ]
                }))?
            }
            Self::BookSnapshot(value) => checked_sum(
                value
                    .bids
                    .as_slice()
                    .iter()
                    .chain(value.asks.as_slice())
                    .flat_map(|level| {
                        [
                            level.price.0.retained_bytes(),
                            level.quantity.0.retained_bytes(),
                        ]
                    }),
            )?,
            Self::BookDelta(value) => {
                checked_sum(value.changes.as_slice().iter().flat_map(|change| {
                    [
                        change.level.price.0.retained_bytes(),
                        change.level.quantity.0.retained_bytes(),
                    ]
                }))?
            }
            Self::Auction {
                provider_code,
                rule,
                price,
                paired_quantity,
                ..
            } => checked_sum([
                provider_code.as_str().len(),
                rule.provider_rule().as_str().len(),
                price.as_ref().map_or(0, |value| value.0.retained_bytes()),
                paired_quantity.0.retained_bytes(),
            ])?,
            Self::TradingHalt { status, reason, .. } => checked_sum([
                status.status.as_str().len(),
                status.rule.provider_rule().as_str().len(),
                reason.as_str().len(),
            ])?,
            Self::InstrumentStatus { status, .. } => checked_sum([
                status.status.as_str().len(),
                status.rule.provider_rule().as_str().len(),
            ])?,
            Self::CorporateAction {
                action_id, rule, ..
            } => checked_sum([
                action_id.as_str().len(),
                rule.provider_rule().as_str().len(),
            ])?,
        };
        Ok(bytes)
    }
}
