//! Native paper publication from retained original worker evidence, without import fabrication.
use super::model::{
    AccountObservation, BasisResolution, CostBasisObservation, HoldingObservation,
    NativePaperIdentity, PortfolioTransaction, PublishedRevision, SignedQuantity,
};
use super::{PortfolioApplicationService, PortfolioApplicationServiceError as Error, Runtime};
use market_squawk_adapter_paper::PaperPortfolioReplay;
use market_squawk_adapter_portfolio::{LotMethod, TransactionKind};
use market_squawk_data::{CorporateActionLimits, CorporateActionPlan};
use market_squawk_domain::{AccountId, Money, OrderSide, SourceId, SourceIdentifier, Timestamp};
use market_squawk_portfolio::{PortfolioLimits, PortfolioRevision};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Weak, atomic::Ordering},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(super) const NATIVE_NAMESPACE: &str = "portfolio/native-paper";
const MAGIC: &[u8; 8] = b"MSQPPR01";

/// The sole running paper owner may publish its original worker evidence. No request JSON
/// constructs this capability and no publication grants order or live-source authority.
#[derive(Clone)]
pub(crate) struct PaperPortfolioPublishCapability {
    runtime: Weak<Runtime>,
}
impl std::fmt::Debug for PaperPortfolioPublishCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaperPortfolioPublishCapability")
            .finish_non_exhaustive()
    }
}
impl PortfolioApplicationService {
    pub(crate) fn paper_publisher(&self) -> PaperPortfolioPublishCapability {
        PaperPortfolioPublishCapability {
            runtime: Arc::downgrade(&self.runtime),
        }
    }
}
impl PaperPortfolioPublishCapability {
    /// Seeds the new execution reader before it escapes composition. Imported account state
    /// cannot be substituted for an original native paper head.
    pub(crate) fn current_revision(
        &self,
        account: AccountId,
    ) -> Result<Option<PortfolioRevision>, Error> {
        let runtime = self.runtime.upgrade().ok_or(Error::Cancelled)?;
        let _guard = runtime.admit()?;
        if runtime.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let image = runtime.image.load();
        match image
            .accounts
            .get(&account)
            .and_then(|history| history.revisions.last())
        {
            Some(revision) if revision.native_paper.is_some() => Ok(Some(revision.core.clone())),
            Some(_) => Err(Error::InvalidRequest),
            None => Ok(None),
        }
    }
    /// Reopens the exact native receipt behind the current account head. This is a bounded
    /// read under the existing owned worker; it acquires no repository writer or market authority.
    pub(crate) async fn original_start_terms(
        &self,
        account: AccountId,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<(Money, u32)>, Error> {
        let runtime = self.runtime.upgrade().ok_or(Error::Cancelled)?;
        let guard = runtime.admit()?;
        ensure_live(&runtime, deadline, &cancellation)?;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            ensure_live(&runtime, deadline, &cancellation)?;
            let image = runtime.image.load_full();
            let Some(published) = image.accounts.get(&account).and_then(|history| history.revisions.last()) else {
                return Ok(None);
            };
            let native = published.native_paper.ok_or(Error::InvalidRequest)?;
            let reference = format!("{}/{}.bin", NATIVE_NAMESPACE, hex(&published.artifact_sha256));
            let bytes = super::import::read_artifact(&runtime.artifacts, &reference, runtime.limits.max_artifact_bytes)?;
            if <[u8; 32]>::from(Sha256::digest(&bytes)) != published.artifact_sha256 {
                return Err(Error::CorruptPublication);
            }
            let (receipt, replay, plan) = NativeReceipt::decode(&bytes, runtime.limits)?;
            if receipt.account != account || receipt.revision != published.token().bytes()
                || receipt.as_of != published.effective_at
                || replay.checkpoint_digest() != native.checkpoint_digest
                || replay.snapshot().configuration_digest() != native.configuration_digest
                || replay.snapshot().sequence() != native.sequence
                || original_origin_digest(&replay)? != native.origin_sha256
            {
                return Err(Error::CorruptPublication);
            }
            replay.verify_action_plan(plan.as_ref()).map_err(|_| Error::CorruptPublication)?;
            let (_, accounts) = replay.snapshot().original_accounts().ok_or(Error::CorruptPublication)?;
            let original = accounts.iter().find(|original| original.account_id == account)
                .ok_or(Error::CorruptPublication)?;
            if original.cash.len() != 1 { return Err(Error::InvalidRequest); }
            let cash = original.cash[0];
            let terms = replay.snapshot().simulation();
            if terms.maker_fee_basis_points() != terms.taker_fee_basis_points()
                || !terms.minimum_fee().amount().is_zero()
                || terms.maximum_fee().is_some()
                || terms.minimum_fee().currency() != cash.currency()
                || published.core.base_currency() != cash.currency()
            { return Err(Error::InvalidRequest); }
            let current = runtime.image.load_full();
            if current.accounts.get(&account).and_then(|history| history.revisions.last())
                .is_none_or(|head| head.token() != published.token())
            { return Err(Error::InvalidRequest); }
            ensure_live(&runtime, deadline, &cancellation)?;
            Ok(Some((cash, terms.maker_fee_basis_points())))
        }).await.map_err(|_| Error::Publication)?
    }

    /// Initializes the original paper checkpoint and publishes its native cash revision in one
    /// existing owned blocking operation. Custody retains the sole paper start fence and writer
    /// even when the request waiter is dropped. A checkpoint committed before a later failure is
    /// reopened by the next confirmed attempt, never replaced by another initial deposit.
    pub(crate) async fn publish_initialized_account<F, C>(
        &self,
        account: AccountId,
        deadline: Instant,
        cancellation: CancellationToken,
        initialize: F,
    ) -> Result<PortfolioRevision, Error>
    where
        F: FnOnce() -> Result<(PaperPortfolioReplay, C), Error> + Send + 'static,
        C: Send + 'static,
    {
        let runtime = self.runtime.upgrade().ok_or(Error::Cancelled)?;
        let guard = runtime.admit()?;
        ensure_live(&runtime, deadline, &cancellation)?;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            ensure_live(&runtime, deadline, &cancellation)?;
            let (replay, _custody) = initialize()?;
            ensure_live(&runtime, deadline, &cancellation)?;
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Publication)?
                .as_nanos();
            let as_of =
                Timestamp::from_unix_nanos(i64::try_from(nanos).map_err(|_| Error::Publication)?);
            let mut authority = runtime.authority.lock().map_err(|_| Error::Authority)?;
            let (revision, image) = authority.publish_native_paper(
                &runtime.artifacts,
                account,
                &replay,
                None,
                as_of,
                || ensure_live(&runtime, deadline, &cancellation),
            )?;
            runtime.image.store(Arc::new(image));
            Ok(revision)
        })
        .await
        .map_err(|_| Error::Publication)?
    }

    pub(crate) async fn publish(
        &self,
        replay: PaperPortfolioReplay,
        account: AccountId,
        plan: Option<CorporateActionPlan>,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioRevision, Error> {
        let runtime = self.runtime.upgrade().ok_or(Error::Cancelled)?;
        let guard = runtime.admit()?;
        ensure_live(&runtime, deadline, &cancellation)?;
        // This owned operation remains counted during caller cancellation/drop and shutdown.
        // It takes only the canonical authority lock, never the paper controller/worker lock.
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            ensure_live(&runtime, deadline, &cancellation)?;
            let mut authority = runtime.authority.lock().map_err(|_| Error::Authority)?;
            let (revision, image) = authority.publish_native_paper(
                &runtime.artifacts,
                account,
                &replay,
                plan.as_ref(),
                as_of,
                || ensure_live(&runtime, deadline, &cancellation),
            )?;
            // Once the durable manifest commits, publish the same image even if cancellation
            // arrives. A later retry/reopen reads the exact committed head.
            runtime.image.store(Arc::new(image));
            Ok(revision)
        })
        .await
        .map_err(|_| Error::Publication)?
    }
}
fn ensure_live(
    runtime: &Runtime,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), Error> {
    if cancellation.is_cancelled()
        || runtime.cancellation.is_cancelled()
        || !runtime.accepting.load(Ordering::Acquire)
    {
        return Err(Error::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(Error::DeadlineExceeded);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeReceipt {
    pub(super) account: AccountId,
    pub(super) as_of: Timestamp,
    pub(super) prior_revision: Option<[u8; 32]>,
    pub(super) revision: [u8; 32],
}
impl NativeReceipt {
    pub(super) fn encode(
        &self,
        replay: &PaperPortfolioReplay,
        plan: Option<&CorporateActionPlan>,
        maximum: usize,
    ) -> Result<Vec<u8>, Error> {
        let header = serde_json::to_vec(self).map_err(|_| Error::Publication)?;
        let original = replay.encode(maximum).map_err(|_| Error::Publication)?;
        let plan = plan
            .map(CorporateActionPlan::encode_recovery_material)
            .transpose()
            .map_err(|_| Error::Publication)?
            .unwrap_or_default();
        let size = 32usize
            .checked_add(header.len())
            .and_then(|n| n.checked_add(original.len()))
            .and_then(|n| n.checked_add(plan.len()))
            .ok_or(Error::ResourceExhausted)?;
        if size > maximum {
            return Err(Error::ResourceExhausted);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| Error::ResourceExhausted)?;
        bytes.extend_from_slice(MAGIC);
        for len in [header.len(), original.len(), plan.len()] {
            bytes.extend_from_slice(
                &u64::try_from(len)
                    .map_err(|_| Error::ResourceExhausted)?
                    .to_be_bytes(),
            );
        }
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&original);
        bytes.extend_from_slice(&plan);
        Ok(bytes)
    }
    pub(super) fn decode(
        bytes: &[u8],
        limits: super::PortfolioApplicationLimits,
    ) -> Result<(Self, PaperPortfolioReplay, Option<CorporateActionPlan>), Error> {
        if bytes.len() > limits.max_artifact_bytes || bytes.len() < 32 || &bytes[..8] != MAGIC {
            return Err(Error::CorruptPublication);
        }
        let mut lengths = [0usize; 3];
        for (i, len) in lengths.iter_mut().enumerate() {
            *len = usize::try_from(u64::from_be_bytes(
                bytes[8 + i * 8..16 + i * 8]
                    .try_into()
                    .map_err(|_| Error::CorruptPublication)?,
            ))
            .map_err(|_| Error::CorruptPublication)?;
        }
        let header_end = 32usize
            .checked_add(lengths[0])
            .ok_or(Error::CorruptPublication)?;
        let replay_end = header_end
            .checked_add(lengths[1])
            .ok_or(Error::CorruptPublication)?;
        if replay_end.checked_add(lengths[2]) != Some(bytes.len())
            || header_end > bytes.len()
            || replay_end > bytes.len()
        {
            return Err(Error::CorruptPublication);
        }
        let receipt: Self = serde_json::from_slice(&bytes[32..header_end])
            .map_err(|_| Error::CorruptPublication)?;
        let replay =
            PaperPortfolioReplay::decode(&bytes[header_end..replay_end], limits.max_artifact_bytes)
                .map_err(|_| Error::CorruptPublication)?;
        let plan = if lengths[2] == 0 {
            None
        } else {
            Some(
                CorporateActionPlan::decode_recovery_material(
                    &bytes[replay_end..],
                    CorporateActionLimits::try_new(
                        NonZeroUsize::new(limits.max_result_items.min(1_000_000))
                            .ok_or(Error::InvalidLimits)?,
                        NonZeroUsize::new(limits.max_retained_bytes.min(512 * 1024 * 1024))
                            .ok_or(Error::InvalidLimits)?,
                    )
                    .map_err(|_| Error::CorruptPublication)?,
                )
                .map_err(|_| Error::CorruptPublication)?,
            )
        };
        Ok((receipt, replay, plan))
    }
}

pub(super) fn build_native_revision(
    replay: &PaperPortfolioReplay,
    account: AccountId,
    plan: Option<&CorporateActionPlan>,
    prior: Option<&PublishedRevision>,
    as_of: Timestamp,
    limits: PortfolioLimits,
    artifact_sha256: [u8; 32],
) -> Result<PublishedRevision, Error> {
    if prior.is_none() && !replay.permits_history_genesis() {
        // A previously durable checkpoint can have purged zero-net-value round trips. Its
        // current balances are not proof of complete original transaction history.
        return Err(Error::CorruptPublication);
    }
    let snapshot = replay.snapshot();
    let origin_sha256 = original_origin_digest(replay)?;
    let accounting_sha256 = accounting_digest(replay)?;
    replay
        .verify_action_plan(plan)
        .map_err(|_| Error::CorruptPublication)?;
    if prior.is_some_and(|old| old.native_paper.is_none() || old.effective_at > as_of)
        || prior.and_then(|old| old.native_paper).is_some_and(|old| {
            old.sequence > snapshot.sequence() || old.origin_sha256 != origin_sha256
        })
    {
        return Err(Error::CorruptPublication);
    }
    let previous_sequence = prior
        .and_then(|old| old.native_paper)
        .map(|old| old.sequence);
    let core = crate::paper_bot::build_paper_revision(
        snapshot,
        account,
        plan,
        prior.map(|old| &old.core),
        previous_sequence,
        as_of,
        limits,
        replay.checkpoint_digest(),
    )
    .map_err(|_| Error::Publication)?;
    let source_id = SourceId::try_from("native-paper-execution").map_err(|_| Error::Publication)?;
    let source_reference = SourceIdentifier::try_from(format!(
        "paper-checkpoint-{}",
        hex(&replay.checkpoint_digest())
    ))
    .map_err(|_| Error::Publication)?;
    let account_row = AccountObservation {
        account_id: account,
        currency: core.base_currency(),
        cash_balance: core.cash(),
        settlement_available_cash: Some(
            replay
                .available_cash(account, core.base_currency())
                .map_err(|_| Error::Publication)?,
        ),
        as_of,
        source_reference: source_reference.clone(),
    };
    let holdings = core
        .positions()
        .iter()
        .map(|position| {
            let mark = snapshot
                .executable_marks()
                .iter()
                .find(|mark| mark.execution_terms().instrument_id() == position.instrument_id())
                .ok_or(Error::Publication)?;
            let basis =
                position
                    .cost_basis()
                    .complete()
                    .map_or(BasisResolution::Missing, |amount| {
                        BasisResolution::Resolved {
                            observation: CostBasisObservation {
                                account_id: account,
                                instrument_id: position.instrument_id(),
                                amount,
                                lot_method: LotMethod::AverageCost,
                                source_reference: source_reference.clone(),
                            },
                        }
                    });
            Ok(HoldingObservation {
                account_id: account,
                instrument_id: position.instrument_id(),
                currency: position.market_value().currency(),
                quantity: SignedQuantity(position.quantity()),
                lot_size: mark.execution_terms().lot_size(),
                market_value: position.market_value(),
                as_of: mark.observed_at(),
                basis,
                source_reference: source_reference.clone(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let mut transactions = prior.map_or_else(Vec::new, |old| old.transactions.clone());
    if prior.is_none() {
        let (opened_at, accounts) = snapshot.original_accounts().ok_or(Error::Publication)?;
        let original = accounts
            .iter()
            .find(|original| original.account_id == account)
            .ok_or(Error::Publication)?;
        if original.cash.len() != 1 {
            return Err(Error::Publication);
        }
        transactions.push(PortfolioTransaction {
            broker_transaction_id: identifier("paper-sandbox-initial-cash-deposit")?,
            account_id: account,
            instrument_id: None,
            kind: TransactionKind::CashTransfer,
            amount: original.cash[0],
            quantity: None,
            occurred_at: opened_at,
            lot_method: None,
            source_reference: source_reference.clone(),
        });
    }
    for fill in snapshot
        .fills()
        .iter()
        .filter(|fill| previous_sequence.is_none_or(|sequence| fill.sequence() > sequence))
    {
        let order = snapshot
            .orders()
            .iter()
            .find(|order| order.order_id() == fill.order_id())
            .ok_or(Error::Publication)?;
        if order.account_id() != account {
            continue;
        }
        let terms = order.execution_terms();
        let units = Decimal::from(fill.quantity().get())
            .checked_mul(terms.lot_size().as_decimal())
            .ok_or(Error::Publication)?;
        let buy = order.side() == OrderSide::Buy;
        transactions.push(PortfolioTransaction {
            broker_transaction_id: identifier(&format!("paper-fill-{:020}", fill.sequence()))?,
            account_id: account,
            instrument_id: Some(terms.instrument_id()),
            kind: TransactionKind::Trade,
            amount: Money::new(
                if buy {
                    -fill.notional().amount()
                } else {
                    fill.notional().amount()
                },
                fill.notional().currency(),
            ),
            quantity: Some(SignedQuantity(if buy { units } else { -units })),
            occurred_at: fill.event_at(),
            lot_method: Some(LotMethod::AverageCost),
            source_reference: source_reference.clone(),
        });
        if !fill.fee().amount().is_zero() {
            transactions.push(PortfolioTransaction {
                broker_transaction_id: identifier(&format!(
                    "paper-fill-fee-{:020}",
                    fill.sequence()
                ))?,
                account_id: account,
                instrument_id: Some(terms.instrument_id()),
                kind: TransactionKind::Fee,
                amount: Money::new(-fill.fee().amount(), fill.fee().currency()),
                quantity: None,
                occurred_at: fill.event_at(),
                lot_method: None,
                source_reference: source_reference.clone(),
            });
        }
    }
    for claim in snapshot.cash_entitlements().iter().filter(|claim| {
        claim.account_id() == account && claim.settled() && !claim.amount().amount().is_zero()
    }) {
        let id = identifier(&format!("paper-action-cash-{}", hex(&claim.evidence())))?;
        if transactions
            .iter()
            .any(|row| row.broker_transaction_id == id)
        {
            continue;
        }
        transactions.push(PortfolioTransaction {
            broker_transaction_id: id,
            account_id: account,
            instrument_id: Some(claim.instrument_id()),
            kind: TransactionKind::Income,
            amount: claim.amount(),
            quantity: None,
            occurred_at: claim.simulated_settlement_at().ok_or(Error::Publication)?,
            lot_method: None,
            source_reference: source_reference.clone(),
        });
    }
    transactions.sort_by(|a, b| {
        a.occurred_at
            .cmp(&b.occurred_at)
            .then_with(|| a.broker_transaction_id.cmp(&b.broker_transaction_id))
    });
    Ok(PublishedRevision {
        core,
        account: account_row,
        holdings,
        transactions,
        discrepancies: Vec::new(),
        source_id: source_id.clone(),
        source_coverage: vec![source_id],
        effective_at: as_of,
        available_at: Some(as_of),
        artifact_sha256,
        native_paper: Some(NativePaperIdentity {
            configuration_digest: snapshot.configuration_digest(),
            checkpoint_digest: replay.checkpoint_digest(),
            sequence: snapshot.sequence(),
            origin_sha256,
            accounting_sha256,
            accounting_anchor: prior.is_none_or(|old| {
                old.native_paper.is_none_or(|native| {
                    native.accounting_sha256 != accounting_sha256
                        || snapshot
                            .fills()
                            .iter()
                            .any(|fill| fill.sequence() > native.sequence)
                })
            }),
        }),
    })
}
fn identifier(value: &str) -> Result<SourceIdentifier, Error> {
    SourceIdentifier::try_from(value).map_err(|_| Error::Publication)
}
fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn original_origin_digest(replay: &PaperPortfolioReplay) -> Result<[u8; 32], Error> {
    let original = replay
        .snapshot()
        .original_accounts()
        .ok_or(Error::Publication)?;
    Ok(Sha256::digest(serde_json::to_vec(&original).map_err(|_| Error::Publication)?).into())
}
fn accounting_digest(replay: &PaperPortfolioReplay) -> Result<[u8; 32], Error> {
    let snapshot = replay.snapshot();
    let cash = snapshot
        .cash()
        .iter()
        .map(|value| (value.account_id(), value.balance()))
        .collect::<Vec<_>>();
    let positions = snapshot
        .positions()
        .iter()
        .map(|value| {
            (
                value.account_id(),
                value.instrument_id(),
                value.lots(),
                value.cost_basis(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Sha256::digest(
        serde_json::to_vec(&(
            original_origin_digest(replay)?,
            cash,
            positions,
            snapshot.cash_entitlements(),
        ))
        .map_err(|_| Error::Publication)?,
    )
    .into())
}
