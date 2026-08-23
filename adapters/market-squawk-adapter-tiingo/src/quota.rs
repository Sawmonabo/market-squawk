use std::collections::BTreeSet;
use std::num::NonZeroU64;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::TiingoTicker;

/// Verified Tiingo Starter request ceiling per rolling/provider-defined hour.
pub const TIINGO_PROVIDER_REQUESTS_PER_HOUR: u64 = 50;
/// Verified Tiingo Starter request ceiling per provider-defined day.
pub const TIINGO_PROVIDER_REQUESTS_PER_DAY: u64 = 1_000;
/// Verified Tiingo Starter unique-symbol ceiling per provider-defined month.
pub const TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH: u64 = 500;
/// Verified Tiingo Starter response-bandwidth ceiling per provider-defined month.
pub const TIINGO_PROVIDER_BYTES_PER_MONTH: u64 = 1_000_000_000;

/// Lower Market Squawk application policy per conservative hour window.
pub const TIINGO_APPLICATION_REQUESTS_PER_HOUR: u64 = 40;
/// Lower Market Squawk application policy per conservative day window.
pub const TIINGO_APPLICATION_REQUESTS_PER_DAY: u64 = 800;
/// Lower Market Squawk application policy per conservative month window.
pub const TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH: u64 = 400;
/// Lower Market Squawk application policy using decimal provider bandwidth units.
pub const TIINGO_APPLICATION_BYTES_PER_MONTH: u64 = 800_000_000;

/// Tiingo quota-ledger invariant failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TiingoQuotaError {
    /// A provider-window reset instant was absent, not future, or not ordered hour/day/month.
    #[error("invalid Tiingo quota window")]
    InvalidWindow,
    /// Persisted counters violated lower admission policy or the absolute provider ceiling.
    #[error("invalid Tiingo persisted quota state")]
    InvalidPersistedState,
    /// A checked quota counter or retained byte total overflowed.
    #[error("Tiingo quota arithmetic overflow")]
    Overflow,
    /// Actual response bytes exceeded the admitted reservation; the request is charged and the
    /// provider remains unavailable until the ledger is conservatively reconciled.
    #[error("Tiingo response bytes exceeded the quota reservation")]
    ResponseExceededReservation,
    /// A permit was committed for a different provider ticker.
    #[error("Tiingo quota permit symbol mismatch")]
    SymbolMismatch,
    /// A crash-retained response reservation must be reconciled before another request or reset.
    #[error("Tiingo quota response reservation requires reconciliation")]
    PendingResponseUnreconciled,
}

/// Explicit provider-window boundaries supplied by durable scheduler authority.
///
/// Reset time zones/instants are unpublished. The adapter does not calculate or optimistically
/// advance these coordinates; an operator-reviewed or conservatively elapsed policy must supply
/// them and persist them beside the counters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TiingoQuotaWindows {
    hour_resets_at: Timestamp,
    day_resets_at: Timestamp,
    month_resets_at: Timestamp,
}

impl TiingoQuotaWindows {
    /// Constructs strictly future and increasingly broad reset coordinates.
    pub fn try_new(
        observed_at: Timestamp,
        hour_resets_at: Timestamp,
        day_resets_at: Timestamp,
        month_resets_at: Timestamp,
    ) -> Result<Self, TiingoQuotaError> {
        if hour_resets_at <= observed_at
            || day_resets_at < hour_resets_at
            || month_resets_at < day_resets_at
        {
            return Err(TiingoQuotaError::InvalidWindow);
        }
        Ok(Self {
            hour_resets_at,
            day_resets_at,
            month_resets_at,
        })
    }

    /// Returns the conservative hour reset coordinate.
    pub const fn hour_resets_at(self) -> Timestamp {
        self.hour_resets_at
    }

    /// Returns the conservative day reset coordinate.
    pub const fn day_resets_at(self) -> Timestamp {
        self.day_resets_at
    }

    /// Returns the conservative month reset coordinate.
    pub const fn month_resets_at(self) -> Timestamp {
        self.month_resets_at
    }
}

/// Serializable durable counters and exact monthly unique-symbol membership.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TiingoQuotaSnapshot {
    windows: TiingoQuotaWindows,
    requests_this_hour: u64,
    requests_this_day: u64,
    response_bytes_this_month: u64,
    unique_symbols_this_month: BTreeSet<TiingoTicker>,
    pending_response: Option<TiingoPendingResponseReservation>,
    state_version: u64,
}

impl TiingoQuotaSnapshot {
    /// Returns the exact window evidence owning these counters.
    pub const fn windows(&self) -> TiingoQuotaWindows {
        self.windows
    }

    /// Returns request attempts charged in the current conservative hour.
    pub const fn requests_this_hour(&self) -> u64 {
        self.requests_this_hour
    }

    /// Returns request attempts charged in the current conservative day.
    pub const fn requests_this_day(&self) -> u64 {
        self.requests_this_day
    }

    /// Returns actual response bytes charged in the current conservative month.
    pub const fn response_bytes_this_month(&self) -> u64 {
        self.response_bytes_this_month
    }

    /// Returns exact provider tickers charged as monthly unique symbols.
    pub const fn unique_symbols_this_month(&self) -> &BTreeSet<TiingoTicker> {
        &self.unique_symbols_this_month
    }

    /// Returns monotonically increasing compare-and-swap state version.
    pub const fn state_version(&self) -> u64 {
        self.state_version
    }

    /// Returns the crash-retained response reservation, when an admitted request did not finish.
    pub const fn pending_response(&self) -> Option<&TiingoPendingResponseReservation> {
        self.pending_response.as_ref()
    }

    /// Returns a canonical identity for durable persistence/restart comparison.
    pub fn digest(&self) -> EvidenceDigest {
        let mut hasher = Sha256::new();
        hasher.update(b"tiingo.quota-snapshot.v2");
        for value in [
            self.windows.hour_resets_at().unix_nanos(),
            self.windows.day_resets_at().unix_nanos(),
            self.windows.month_resets_at().unix_nanos(),
        ] {
            hasher.update(value.to_be_bytes());
        }
        for value in [
            self.requests_this_hour,
            self.requests_this_day,
            self.response_bytes_this_month,
            self.state_version,
        ] {
            hasher.update(value.to_be_bytes());
        }
        for symbol in &self.unique_symbols_this_month {
            hasher.update(
                u64::try_from(symbol.as_str().len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            hasher.update(symbol.as_str().as_bytes());
        }
        match &self.pending_response {
            Some(pending) => {
                hasher.update([1]);
                hasher.update(
                    u64::try_from(pending.ticker.as_str().len())
                        .unwrap_or(u64::MAX)
                        .to_be_bytes(),
                );
                hasher.update(pending.ticker.as_str().as_bytes());
                hasher.update(pending.reserved_response_bytes.get().to_be_bytes());
            }
            None => hasher.update([0]),
        }
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
    }
}

/// Serializable crash marker for the one request serialized through this provider credential.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TiingoPendingResponseReservation {
    ticker: TiingoTicker,
    reserved_response_bytes: NonZeroU64,
}

impl TiingoPendingResponseReservation {
    /// Returns the exact provider ticker whose request attempt was durably charged.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the conservative response-byte reservation retained across a crash.
    pub const fn reserved_response_bytes(&self) -> NonZeroU64 {
        self.reserved_response_bytes
    }
}

/// The first exhausted lower application-policy dimension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoQuotaAdmission {
    /// Every conjunctive quota dimension can admit the proposed attempt.
    Admitted,
    /// 40 application requests/hour would be exceeded.
    HourlyRequestsExhausted,
    /// 800 application requests/day would be exceeded.
    DailyRequestsExhausted,
    /// 400 application-unique symbols/month would be exceeded.
    MonthlyUniqueSymbolsExhausted,
    /// The caller's maximum possible response would exceed 800 MB/month.
    MonthlyBandwidthExhausted,
    /// A prior process stopped after dispatch admission and before exact byte settlement.
    PendingResponseUnreconciled,
}

/// In-memory transition engine over a snapshot that must be durably compare-and-swap persisted by
/// the shared provider-rate authority before any request is sent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoQuotaLedger {
    snapshot: TiingoQuotaSnapshot,
}

impl TiingoQuotaLedger {
    /// Creates empty counters under explicit conservative reset boundaries.
    pub fn new(windows: TiingoQuotaWindows) -> Self {
        Self {
            snapshot: TiingoQuotaSnapshot {
                windows,
                requests_this_hour: 0,
                requests_this_day: 0,
                response_bytes_this_month: 0,
                unique_symbols_this_month: BTreeSet::new(),
                pending_response: None,
                state_version: 1,
            },
        }
    }

    /// Restores persisted state after validating admission invariants and absolute ceilings.
    ///
    /// A settled provider overage may exceed the lower bandwidth policy but never the provider
    /// ceiling; subsequent classification remains denied until a governed month reset.
    pub fn try_restore(snapshot: TiingoQuotaSnapshot) -> Result<Self, TiingoQuotaError> {
        let pending_bytes = snapshot
            .pending_response
            .as_ref()
            .map_or(0, |pending| pending.reserved_response_bytes.get());
        let pending_symbol_is_charged = snapshot
            .pending_response
            .as_ref()
            .is_none_or(|pending| snapshot.unique_symbols_this_month.contains(&pending.ticker));
        if snapshot.requests_this_hour > TIINGO_APPLICATION_REQUESTS_PER_HOUR
            || snapshot.requests_this_day > TIINGO_APPLICATION_REQUESTS_PER_DAY
            || snapshot.response_bytes_this_month > TIINGO_PROVIDER_BYTES_PER_MONTH
            || snapshot
                .response_bytes_this_month
                .checked_add(pending_bytes)
                .is_none_or(|bytes| bytes > TIINGO_PROVIDER_BYTES_PER_MONTH)
            || (snapshot.pending_response.is_some()
                && snapshot
                    .response_bytes_this_month
                    .checked_add(pending_bytes)
                    .is_none_or(|bytes| bytes > TIINGO_APPLICATION_BYTES_PER_MONTH))
            || !pending_symbol_is_charged
            || u64::try_from(snapshot.unique_symbols_this_month.len())
                .map_err(|_| TiingoQuotaError::InvalidPersistedState)?
                > TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH
            || snapshot.state_version == 0
        {
            return Err(TiingoQuotaError::InvalidPersistedState);
        }
        Ok(Self { snapshot })
    }

    /// Returns exact serializable persistent state.
    pub const fn snapshot(&self) -> &TiingoQuotaSnapshot {
        &self.snapshot
    }

    /// Checks all quota dimensions without mutation.
    pub fn classify(
        &self,
        ticker: &TiingoTicker,
        reserved_response_bytes: NonZeroU64,
    ) -> Result<TiingoQuotaAdmission, TiingoQuotaError> {
        if self.snapshot.pending_response.is_some() {
            return Ok(TiingoQuotaAdmission::PendingResponseUnreconciled);
        }
        if self
            .snapshot
            .requests_this_hour
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?
            > TIINGO_APPLICATION_REQUESTS_PER_HOUR
        {
            return Ok(TiingoQuotaAdmission::HourlyRequestsExhausted);
        }
        if self
            .snapshot
            .requests_this_day
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?
            > TIINGO_APPLICATION_REQUESTS_PER_DAY
        {
            return Ok(TiingoQuotaAdmission::DailyRequestsExhausted);
        }
        let new_symbol = !self.snapshot.unique_symbols_this_month.contains(ticker);
        if new_symbol
            && u64::try_from(self.snapshot.unique_symbols_this_month.len())
                .map_err(|_| TiingoQuotaError::Overflow)?
                .checked_add(1)
                .ok_or(TiingoQuotaError::Overflow)?
                > TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH
        {
            return Ok(TiingoQuotaAdmission::MonthlyUniqueSymbolsExhausted);
        }
        if self
            .snapshot
            .response_bytes_this_month
            .checked_add(reserved_response_bytes.get())
            .ok_or(TiingoQuotaError::Overflow)?
            > TIINGO_APPLICATION_BYTES_PER_MONTH
        {
            return Ok(TiingoQuotaAdmission::MonthlyBandwidthExhausted);
        }
        Ok(TiingoQuotaAdmission::Admitted)
    }

    /// Reserves request and unique-symbol capacity before transport.
    ///
    /// The returned snapshot must be transactionally persisted by expected prior state version
    /// before sending. Request attempts remain charged on provider error or decode failure.
    pub fn reserve(
        &mut self,
        ticker: TiingoTicker,
        reserved_response_bytes: NonZeroU64,
    ) -> Result<Result<TiingoQuotaPermit, TiingoQuotaAdmission>, TiingoQuotaError> {
        let admission = self.classify(&ticker, reserved_response_bytes)?;
        if admission != TiingoQuotaAdmission::Admitted {
            return Ok(Err(admission));
        }
        let prior_state_version = self.snapshot.state_version;
        self.snapshot.requests_this_hour = self
            .snapshot
            .requests_this_hour
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;
        self.snapshot.requests_this_day = self
            .snapshot
            .requests_this_day
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;
        let introduced_monthly_ticker = self
            .snapshot
            .unique_symbols_this_month
            .insert(ticker.clone());
        self.snapshot.pending_response = Some(TiingoPendingResponseReservation {
            ticker: ticker.clone(),
            reserved_response_bytes,
        });
        self.snapshot.state_version = self
            .snapshot
            .state_version
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;
        Ok(Ok(TiingoQuotaPermit {
            ticker,
            reserved_response_bytes,
            prior_state_version,
            reserved_state_version: self.snapshot.state_version,
            introduced_monthly_ticker,
        }))
    }

    /// Cancels one exact reservation after aggregate authority proves no request was dispatched.
    ///
    /// The rollback restores request counters and monthly membership while retaining a monotonic
    /// state version. Its resulting snapshot must be compare-and-swap persisted before another
    /// request can be admitted.
    pub fn cancel_undispatched(
        &mut self,
        permit: &TiingoQuotaPermit,
        ticker: &TiingoTicker,
    ) -> Result<(), TiingoQuotaError> {
        if &permit.ticker != ticker {
            return Err(TiingoQuotaError::SymbolMismatch);
        }
        if permit
            .prior_state_version
            .checked_add(1)
            .filter(|version| *version == permit.reserved_state_version)
            .is_none()
            || self.snapshot.state_version != permit.reserved_state_version
        {
            return Err(TiingoQuotaError::InvalidPersistedState);
        }
        let pending = self
            .snapshot
            .pending_response
            .as_ref()
            .ok_or(TiingoQuotaError::InvalidPersistedState)?;
        if pending.ticker != permit.ticker
            || pending.reserved_response_bytes != permit.reserved_response_bytes
            || !self
                .snapshot
                .unique_symbols_this_month
                .contains(&permit.ticker)
        {
            return Err(TiingoQuotaError::InvalidPersistedState);
        }
        let requests_this_hour = self
            .snapshot
            .requests_this_hour
            .checked_sub(1)
            .ok_or(TiingoQuotaError::InvalidPersistedState)?;
        let requests_this_day = self
            .snapshot
            .requests_this_day
            .checked_sub(1)
            .ok_or(TiingoQuotaError::InvalidPersistedState)?;
        let state_version = self
            .snapshot
            .state_version
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;

        self.snapshot.requests_this_hour = requests_this_hour;
        self.snapshot.requests_this_day = requests_this_day;
        if permit.introduced_monthly_ticker {
            self.snapshot
                .unique_symbols_this_month
                .remove(&permit.ticker);
        }
        self.snapshot.pending_response = None;
        self.snapshot.state_version = state_version;
        Ok(())
    }

    /// Charges actual response bytes after bounded body receipt.
    ///
    /// An over-reservation response remains charged up to the absolute provider monthly ceiling
    /// and returns an error; lower application policy then denies any unsafe follow-up request.
    pub fn commit_response(
        &mut self,
        permit: &TiingoQuotaPermit,
        ticker: &TiingoTicker,
        actual_response_bytes: u64,
    ) -> Result<(), TiingoQuotaError> {
        if &permit.ticker != ticker {
            return Err(TiingoQuotaError::SymbolMismatch);
        }
        if self.snapshot.state_version != permit.reserved_state_version {
            return Err(TiingoQuotaError::InvalidPersistedState);
        }
        let pending = self
            .snapshot
            .pending_response
            .as_ref()
            .ok_or(TiingoQuotaError::InvalidPersistedState)?;
        if pending.ticker != permit.ticker
            || pending.reserved_response_bytes != permit.reserved_response_bytes
        {
            return Err(TiingoQuotaError::InvalidPersistedState);
        }
        let response_bytes_this_month = self
            .snapshot
            .response_bytes_this_month
            .checked_add(actual_response_bytes)
            .ok_or(TiingoQuotaError::Overflow)?;
        if response_bytes_this_month > TIINGO_PROVIDER_BYTES_PER_MONTH {
            return Err(TiingoQuotaError::InvalidPersistedState);
        }
        self.snapshot.response_bytes_this_month = response_bytes_this_month;
        self.snapshot.pending_response = None;
        self.snapshot.state_version = self
            .snapshot
            .state_version
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;
        if actual_response_bytes > permit.reserved_response_bytes.get()
            || self.snapshot.response_bytes_this_month > TIINGO_APPLICATION_BYTES_PER_MONTH
        {
            return Err(TiingoQuotaError::ResponseExceededReservation);
        }
        Ok(())
    }

    /// Conservatively settles one crash-retained request at its admitted maximum response size.
    ///
    /// A caller restoring durable state must persist the returned state transition before sending
    /// another Tiingo request. This avoids silently forgetting bandwidth that may have crossed the
    /// socket boundary immediately before a process failure.
    pub fn reconcile_incomplete_response(&mut self) -> Result<bool, TiingoQuotaError> {
        let Some(pending) = self.snapshot.pending_response.as_ref() else {
            return Ok(false);
        };
        let response_bytes_this_month = self
            .snapshot
            .response_bytes_this_month
            .checked_add(pending.reserved_response_bytes.get())
            .filter(|bytes| *bytes <= TIINGO_PROVIDER_BYTES_PER_MONTH)
            .ok_or(TiingoQuotaError::InvalidPersistedState)?;
        self.snapshot.response_bytes_this_month = response_bytes_this_month;
        self.snapshot.pending_response = None;
        self.snapshot.state_version = self
            .snapshot
            .state_version
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;
        Ok(true)
    }

    /// Advances only windows whose exact conservative reset coordinates have elapsed.
    ///
    /// The replacement boundaries must be supplied externally because provider reset rules are
    /// unpublished. This method never computes a timezone or resets an unelapsed counter.
    pub fn advance_windows(
        &mut self,
        observed_at: Timestamp,
        next: TiingoQuotaWindows,
    ) -> Result<(), TiingoQuotaError> {
        if self.snapshot.pending_response.is_some() {
            return Err(TiingoQuotaError::PendingResponseUnreconciled);
        }
        if next.hour_resets_at() <= observed_at
            || next.day_resets_at() < next.hour_resets_at()
            || next.month_resets_at() < next.day_resets_at()
        {
            return Err(TiingoQuotaError::InvalidWindow);
        }
        let old = self.snapshot.windows;
        if observed_at < old.hour_resets_at()
            || next.hour_resets_at() <= old.hour_resets_at()
            || next.day_resets_at() < old.day_resets_at()
            || next.month_resets_at() < old.month_resets_at()
        {
            return Err(TiingoQuotaError::InvalidWindow);
        }
        self.snapshot.requests_this_hour = 0;
        if observed_at >= old.day_resets_at() {
            self.snapshot.requests_this_day = 0;
        }
        if observed_at >= old.month_resets_at() {
            self.snapshot.response_bytes_this_month = 0;
            self.snapshot.unique_symbols_this_month.clear();
        }
        self.snapshot.windows = next;
        self.snapshot.state_version = self
            .snapshot
            .state_version
            .checked_add(1)
            .ok_or(TiingoQuotaError::Overflow)?;
        Ok(())
    }
}

/// One request-attempt reservation. It contains no secret or response body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoQuotaPermit {
    ticker: TiingoTicker,
    reserved_response_bytes: NonZeroU64,
    prior_state_version: u64,
    reserved_state_version: u64,
    introduced_monthly_ticker: bool,
}

impl TiingoQuotaPermit {
    /// Returns exact provider ticker charged to monthly uniqueness.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns caller's maximum admitted response bytes.
    pub const fn reserved_response_bytes(&self) -> NonZeroU64 {
        self.reserved_response_bytes
    }

    /// Returns state version expected when transactionally reserving this request.
    pub const fn prior_state_version(&self) -> u64 {
        self.prior_state_version
    }

    /// Returns state version after request/unique-symbol reservation.
    pub const fn reserved_state_version(&self) -> u64 {
        self.reserved_state_version
    }
}
