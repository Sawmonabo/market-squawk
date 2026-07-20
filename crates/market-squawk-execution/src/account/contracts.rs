//! Startup account, replay-fence, and coordinator configuration contracts.

use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_domain::{AccountId, ClientOrderId, InstrumentId, Money, OrderId};
use thiserror::Error;

use crate::OrderIntentDigest;

/// Startup-fixed memory and partition bounds for authoritative account coordination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountCoordinatorConfig {
    /// Number of deterministic account partitions.
    pub partition_count: NonZeroUsize,
    /// Maximum accounts retained in one partition.
    pub max_accounts_per_partition: NonZeroUsize,
    /// Maximum outstanding and terminal reservation records retained before compaction.
    pub max_reservations_per_account: NonZeroUsize,
    /// Maximum positions retained for one account.
    pub max_positions_per_account: NonZeroUsize,
    /// Maximum consumed client-order identities retained for one account.
    pub max_idempotency_keys_per_account: NonZeroUsize,
    /// Maximum signal-to-expiration duration retained by the exact replay fence.
    pub maximum_intent_lifetime_nanos: NonZeroU64,
    /// Maximum accepted timestamps retained for rate enforcement.
    pub max_rate_events_per_account: NonZeroUsize,
}

impl Default for AccountCoordinatorConfig {
    fn default() -> Self {
        Self {
            partition_count: nonzero_usize(16),
            max_accounts_per_partition: nonzero_usize(1_024),
            max_reservations_per_account: nonzero_usize(256),
            max_positions_per_account: nonzero_usize(4_096),
            max_idempotency_keys_per_account: nonzero_usize(4_096),
            maximum_intent_lifetime_nanos: NonZeroU64::new(86_400_000_000_000)
                .unwrap_or(NonZeroU64::MIN),
            max_rate_events_per_account: nonzero_usize(1_024),
        }
    }
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

/// One explicit startup account state transferred into coordinator ownership.
///
/// Evaluation never accepts this structure. Once admitted, the coordinator is the only current
/// account authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountBootstrap {
    /// Stable account identity.
    pub account_id: AccountId,
    /// Nonzero upstream account revision.
    pub revision: NonZeroU64,
    /// Whether the account is currently eligible for orders.
    pub eligible: bool,
    /// Current available cash before new reservations.
    pub cash: Money,
    /// Current risk capital.
    pub capital: Money,
    /// Highest capital used for drawdown measurement.
    pub peak_capital: Money,
    /// Current gross exposure before new reservations.
    pub gross_exposure: Money,
    /// Current nonnegative realized loss measure.
    pub realized_loss: Money,
    /// Current signed positions in instrument lots.
    pub positions: Vec<(InstrumentId, i64)>,
    /// Authoritative non-expired replay fence loaded before live risk admission.
    pub idempotency: AccountIdempotencyBootstrap,
}

/// Exact restart-loadable replay tombstone for one previously consumed intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountIdempotencyTombstone {
    pub(super) order_id: OrderId,
    pub(super) client_order_id: ClientOrderId,
    pub(super) intent_digest: OrderIntentDigest,
    pub(super) intent_expires_at: market_squawk_domain::Timestamp,
}

impl AccountIdempotencyTombstone {
    /// Constructs an exact persisted identity binding. Temporal validation occurs when the owning
    /// account bootstrap is admitted against the trusted process clock and configured horizon.
    pub fn new(
        order_id: OrderId,
        client_order_id: ClientOrderId,
        intent_digest: OrderIntentDigest,
        intent_expires_at: market_squawk_domain::Timestamp,
    ) -> Self {
        Self {
            order_id,
            client_order_id,
            intent_digest,
            intent_expires_at,
        }
    }

    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    pub const fn client_order_id(&self) -> &ClientOrderId {
        &self.client_order_id
    }

    pub const fn intent_digest(&self) -> OrderIntentDigest {
        self.intent_digest
    }

    pub const fn intent_expires_at(&self) -> market_squawk_domain::Timestamp {
        self.intent_expires_at
    }
}

/// Injected replay-fence bootstrap and persistence snapshot payload.
///
/// This contract performs no I/O. A paper/live composition owner must durably persist a returned
/// snapshot and inject it into the next [`AccountBootstrap`] before enabling live admission. The
/// revision advances monotonically whenever an identity is consumed or expired identities are
/// compacted; durable storage must reject revision rollback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountIdempotencyBootstrap {
    pub(super) revision: NonZeroU64,
    pub(super) tombstones: Box<[AccountIdempotencyTombstone]>,
}

impl AccountIdempotencyBootstrap {
    /// Validates uniqueness of both replay identities inside one authoritative snapshot.
    pub fn try_new(
        revision: NonZeroU64,
        mut tombstones: Vec<AccountIdempotencyTombstone>,
    ) -> Result<Self, AccountIdempotencyBootstrapError> {
        tombstones.sort_unstable_by_key(|tombstone| tombstone.order_id);
        if tombstones
            .windows(2)
            .any(|pair| pair[0].order_id == pair[1].order_id)
        {
            return Err(AccountIdempotencyBootstrapError::DuplicateOrder);
        }
        tombstones.sort_unstable_by(|left, right| left.client_order_id.cmp(&right.client_order_id));
        if tombstones
            .windows(2)
            .any(|pair| pair[0].client_order_id == pair[1].client_order_id)
        {
            return Err(AccountIdempotencyBootstrapError::DuplicateClientOrder);
        }
        Ok(Self {
            revision,
            tombstones: tombstones.into_boxed_slice(),
        })
    }

    /// Creates an empty first replay-fence revision for a new account.
    pub fn empty() -> Self {
        Self {
            revision: NonZeroU64::MIN,
            tombstones: Box::new([]),
        }
    }

    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    pub const fn tombstones(&self) -> &[AccountIdempotencyTombstone] {
        &self.tombstones
    }
}

/// Structural persisted replay-fence failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountIdempotencyBootstrapError {
    #[error("idempotency bootstrap contains a duplicate order identity")]
    DuplicateOrder,
    #[error("idempotency bootstrap contains a duplicate client-order identity")]
    DuplicateClientOrder,
}

/// Atomic account coordination construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountCoordinatorError {
    /// More accounts hashed to a partition than its startup bound permits.
    #[error("account partition capacity exceeded")]
    AccountCapacity,
    /// The same account appeared more than once.
    #[error("duplicate account bootstrap")]
    DuplicateAccount,
    /// A bootstrap violated currency, sign, peak, position, or capacity invariants.
    #[error("invalid account bootstrap")]
    InvalidBootstrap,
    /// Persisted tombstones were expired, beyond the configured horizon, or over capacity.
    #[error("invalid account idempotency bootstrap")]
    InvalidIdempotencyBootstrap,
    /// The startup clock could not validate the persisted replay horizon.
    #[error("trusted clock unavailable during account bootstrap")]
    ClockFailure,
    /// Bounded account-state allocation failed before coordinator publication.
    #[error("bounded account-state allocation failed")]
    Allocation,
}

/// Nonblocking account replay-fence snapshot failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountIdempotencySnapshotError {
    #[error("account does not exist")]
    AccountNotFound,
    #[error("account coordinator partition is busy")]
    Busy,
    #[error("account coordinator partition is poisoned")]
    Poisoned,
    #[error("trusted clock unavailable while compacting replay identities")]
    ClockFailure,
    #[error("idempotency replay-fence revision exhausted")]
    RevisionExhausted,
}
