//! Stopped checkpoint export and fresh restore through the original repository owner.
use super::*;
use crate::{
    FeeSchedule, PaperExecutionConfigInput, PaperExecutionSessionPolicy, PaperExposureValuation,
    PaperVenueSession, PaperVenueSessionCalendar,
};
use market_squawk_domain::{RuleVersion, SourceIdentifier, VenueId};
use std::num::NonZeroU32;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfigWire {
    configuration_version: NonZeroU64,
    deterministic_seed: [u8; 32],
    command_capacity: NonZeroUsize,
    command_maximum_bytes: NonZeroU32,
    market_capacity: NonZeroUsize,
    market_maximum_bytes: NonZeroU32,
    audit_capacity: NonZeroUsize,
    audit_maximum_bytes: NonZeroU32,
    maximum_orders: NonZeroUsize,
    maximum_fills: NonZeroUsize,
    maximum_idempotency_keys: NonZeroUsize,
    maximum_archived_orders: NonZeroUsize,
    matching_work_quantum: NonZeroUsize,
    minimum_latency_nanos: u64,
    maximum_latency_nanos: u64,
    cancel_latency_nanos: u64,
    maximum_mark_age_nanos: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    day_session_calendar: Option<CalendarWire>,
    maximum_participation_basis_points: u32,
    impact_basis_points_per_level: u32,
    reporting_currency: Currency,
    ledger_maximum_accounts: NonZeroUsize,
    ledger_maximum_balances: NonZeroUsize,
    ledger_maximum_positions: NonZeroUsize,
    allow_short: bool,
    exposure_valuation: ExposureWire,
    abort_join_deadline: Duration,
    fee_schedule: FeeWire,
}

impl ConfigWire {
    pub(super) fn from_config(config: &PaperExecutionConfig) -> Self {
        let input = config.input();
        Self {
            configuration_version: input.configuration_version,
            deterministic_seed: input.deterministic_seed,
            command_capacity: input.command_capacity,
            command_maximum_bytes: input.command_maximum_bytes,
            market_capacity: input.market_capacity,
            market_maximum_bytes: input.market_maximum_bytes,
            audit_capacity: input.audit_capacity,
            audit_maximum_bytes: input.audit_maximum_bytes,
            maximum_orders: input.maximum_orders,
            maximum_fills: input.maximum_fills,
            maximum_idempotency_keys: input.maximum_idempotency_keys,
            maximum_archived_orders: input.maximum_archived_orders,
            matching_work_quantum: input.matching_work_quantum,
            minimum_latency_nanos: input.minimum_latency_nanos,
            maximum_latency_nanos: input.maximum_latency_nanos,
            cancel_latency_nanos: input.cancel_latency_nanos,
            maximum_mark_age_nanos: input.maximum_mark_age_nanos,
            day_session_calendar: input
                .session_policy
                .calendar()
                .map(CalendarWire::from_calendar),
            maximum_participation_basis_points: input.maximum_participation_basis_points,
            impact_basis_points_per_level: input.impact_basis_points_per_level,
            reporting_currency: input.reporting_currency,
            ledger_maximum_accounts: input.ledger_maximum_accounts,
            ledger_maximum_balances: input.ledger_maximum_balances,
            ledger_maximum_positions: input.ledger_maximum_positions,
            allow_short: input.allow_short,
            exposure_valuation: ExposureWire::ExecutableExit,
            abort_join_deadline: input.abort_join_deadline,
            fee_schedule: FeeWire::from_fee(input.fee_schedule),
        }
    }
    pub(super) fn to_config(&self) -> Result<PaperExecutionConfig, PaperCheckpointRepositoryError> {
        PaperExecutionConfig::try_new(PaperExecutionConfigInput {
            configuration_version: self.configuration_version,
            deterministic_seed: self.deterministic_seed,
            command_capacity: self.command_capacity,
            command_maximum_bytes: self.command_maximum_bytes,
            market_capacity: self.market_capacity,
            market_maximum_bytes: self.market_maximum_bytes,
            audit_capacity: self.audit_capacity,
            audit_maximum_bytes: self.audit_maximum_bytes,
            maximum_orders: self.maximum_orders,
            maximum_fills: self.maximum_fills,
            maximum_idempotency_keys: self.maximum_idempotency_keys,
            maximum_archived_orders: self.maximum_archived_orders,
            matching_work_quantum: self.matching_work_quantum,
            minimum_latency_nanos: self.minimum_latency_nanos,
            maximum_latency_nanos: self.maximum_latency_nanos,
            cancel_latency_nanos: self.cancel_latency_nanos,
            maximum_mark_age_nanos: self.maximum_mark_age_nanos,
            session_policy: match &self.day_session_calendar {
                Some(calendar) => PaperExecutionSessionPolicy::Venue(calendar.to_calendar()?),
                None => PaperExecutionSessionPolicy::AccountOnly,
            },
            maximum_participation_basis_points: self.maximum_participation_basis_points,
            impact_basis_points_per_level: self.impact_basis_points_per_level,
            reporting_currency: self.reporting_currency,
            ledger_maximum_accounts: self.ledger_maximum_accounts,
            ledger_maximum_balances: self.ledger_maximum_balances,
            ledger_maximum_positions: self.ledger_maximum_positions,
            allow_short: self.allow_short,
            exposure_valuation: match self.exposure_valuation {
                ExposureWire::ExecutableExit => PaperExposureValuation::ExecutableExit,
            },
            abort_join_deadline: self.abort_join_deadline,
            fee_schedule: self.fee_schedule.to_fee()?,
        })
        .map_err(|_| PaperCheckpointRepositoryError::ConfigurationMismatch)
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExposureWire {
    ExecutableExit,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CalendarWire {
    calendar_id: SourceIdentifier,
    ruleset_version: RuleVersion,
    venue_id: VenueId,
    time_zone: String,
    sessions: Vec<(SourceIdentifier, Timestamp, Timestamp)>,
}
impl CalendarWire {
    fn from_calendar(c: &PaperVenueSessionCalendar) -> Self {
        Self {
            calendar_id: c.calendar_id().clone(),
            ruleset_version: c.ruleset_version(),
            venue_id: c.venue_id().clone(),
            time_zone: c.time_zone().to_owned(),
            sessions: c
                .sessions()
                .iter()
                .map(|s| {
                    (
                        s.session_id().clone(),
                        s.opens_at(),
                        s.closes_at_exclusive(),
                    )
                })
                .collect(),
        }
    }
    fn to_calendar(&self) -> Result<PaperVenueSessionCalendar, PaperCheckpointRepositoryError> {
        if self.sessions.len() > crate::MAX_PAPER_VENUE_SESSIONS {
            return Err(PaperCheckpointRepositoryError::ConfigurationMismatch);
        }
        let sessions = self
            .sessions
            .iter()
            .map(|(id, open, close)| PaperVenueSession::try_new(id.clone(), *open, *close))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PaperCheckpointRepositoryError::ConfigurationMismatch)?;
        PaperVenueSessionCalendar::try_new(
            self.calendar_id.clone(),
            self.ruleset_version,
            self.venue_id.clone(),
            &self.time_zone,
            sessions,
        )
        .map_err(|_| PaperCheckpointRepositoryError::ConfigurationMismatch)
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeeWire {
    maker: u32,
    taker: u32,
    minimum: Money,
    maximum: Option<Money>,
    scale: u32,
}
impl FeeWire {
    fn from_fee(f: FeeSchedule) -> Self {
        Self {
            maker: f.maker_basis_points(),
            taker: f.taker_basis_points(),
            minimum: f.minimum_fee(),
            maximum: f.maximum_fee(),
            scale: f.money_scale(),
        }
    }
    fn to_fee(&self) -> Result<FeeSchedule, PaperCheckpointRepositoryError> {
        FeeSchedule::try_new(
            self.maker,
            self.taker,
            self.minimum,
            self.maximum,
            self.scale,
        )
        .map_err(|_| PaperCheckpointRepositoryError::ConfigurationMismatch)
    }
}

const PORTFOLIO_REPLAY_MAGIC: &[u8] = b"market-squawk/paper-portfolio-replay/v1\0";
const PORTFOLIO_REPLAY_HEADER_BYTES: usize = PORTFOLIO_REPLAY_MAGIC.len() + 16 + 32;

/// Read-only original worker state with its exact configuration and checkpoint commitment.
/// It owns no worker, repository, persistence receipt or source-action admission authority.
#[derive(Debug)]
pub struct PaperPortfolioReplay {
    encoded: Vec<u8>,
    snapshot: crate::PaperExecutionSnapshot,
    checkpoint_digest: [u8; 32],
    ledger: crate::PaperLedger,
    permits_history_genesis: bool,
}
impl PaperPortfolioReplay {
    /// Captures one original worker consistency boundary. Running orders are retained unchanged;
    /// quarantined state cannot become a canonical portfolio publication.
    pub fn capture(
        checkpoint: &PaperExecutionCheckpoint,
        configuration: &PaperExecutionConfig,
        maximum_bytes: usize,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        if checkpoint.configuration_digest() != configuration.digest() {
            return Err(PaperCheckpointRepositoryError::ConfigurationMismatch);
        }
        if checkpoint.reconciliation_required() {
            return Err(PaperCheckpointRepositoryError::QuarantinedCheckpoint);
        }
        let checkpoint_bytes = checkpoint.encode(maximum_bytes)?;
        let mut configuration_bytes = BoundedRepositoryWriter::new(maximum_bytes)?;
        serde_json::to_writer(
            &mut configuration_bytes,
            &ConfigWire::from_config(configuration),
        )
        .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
        let configuration_bytes = configuration_bytes.into_inner();
        let total = PORTFOLIO_REPLAY_HEADER_BYTES
            .checked_add(configuration_bytes.len())
            .and_then(|length| length.checked_add(checkpoint_bytes.len()))
            .filter(|length| *length <= maximum_bytes)
            .ok_or(PaperCheckpointError::TooLarge)?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(total)
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        encoded.extend_from_slice(PORTFOLIO_REPLAY_MAGIC);
        encoded.extend_from_slice(
            &u64::try_from(configuration_bytes.len())
                .map_err(|_| PaperCheckpointError::TooLarge)?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(
            &u64::try_from(checkpoint_bytes.len())
                .map_err(|_| PaperCheckpointError::TooLarge)?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&checkpoint.recovery_digest()?);
        encoded.extend_from_slice(&configuration_bytes);
        encoded.extend_from_slice(&checkpoint_bytes);
        drop(configuration_bytes);
        drop(checkpoint_bytes);
        Self::decode_owned(encoded, maximum_bytes)
    }

    /// Preserves original bytes without re-encoding or changing the publication's source state.
    pub fn encode(&self, maximum_bytes: usize) -> Result<Vec<u8>, PaperCheckpointRepositoryError> {
        if self.encoded.len() > maximum_bytes {
            return Err(PaperCheckpointError::TooLarge.into());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.encoded.len())
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        bytes.extend_from_slice(&self.encoded);
        Ok(bytes)
    }

    /// Reuses the original strict config/checkpoint decoders. Historical marks keep their original
    /// clocks; current execution eligibility and original action-source replay remain separate.
    pub fn decode(
        bytes: &[u8],
        maximum_bytes: usize,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        if bytes.len() > maximum_bytes || bytes.len() < PORTFOLIO_REPLAY_HEADER_BYTES {
            return Err(PaperCheckpointError::TooLarge.into());
        }
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        owned.extend_from_slice(bytes);
        Self::decode_owned(owned, maximum_bytes)
    }
    fn decode_owned(
        encoded: Vec<u8>,
        maximum_bytes: usize,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        if encoded.len() > maximum_bytes
            || encoded.len() < PORTFOLIO_REPLAY_HEADER_BYTES
            || !encoded.starts_with(PORTFOLIO_REPLAY_MAGIC)
        {
            return Err(PaperCheckpointRepositoryError::InvalidManifest);
        }
        let offset = PORTFOLIO_REPLAY_MAGIC.len();
        let length = |start| -> Result<usize, PaperCheckpointRepositoryError> {
            let bytes: [u8; 8] = encoded[start..start + 8]
                .try_into()
                .map_err(|_| PaperCheckpointRepositoryError::InvalidManifest)?;
            usize::try_from(u64::from_be_bytes(bytes))
                .map_err(|_| PaperCheckpointError::TooLarge.into())
        };
        let configuration_length = length(offset)?;
        let checkpoint_length = length(offset + 8)?;
        let checkpoint_digest: [u8; 32] = encoded[offset + 16..PORTFOLIO_REPLAY_HEADER_BYTES]
            .try_into()
            .map_err(|_| PaperCheckpointRepositoryError::InvalidManifest)?;
        let split = PORTFOLIO_REPLAY_HEADER_BYTES
            .checked_add(configuration_length)
            .ok_or(PaperCheckpointError::TooLarge)?;
        if configuration_length == 0
            || checkpoint_length == 0
            || split.checked_add(checkpoint_length) != Some(encoded.len())
        {
            return Err(PaperCheckpointRepositoryError::InvalidManifest);
        }
        let configuration: ConfigWire =
            serde_json::from_slice(&encoded[PORTFOLIO_REPLAY_HEADER_BYTES..split])
                .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
        let configuration = configuration.to_config()?;
        let bytes = &encoded[split..];
        if <[u8; 32]>::from(Sha256::digest(bytes)) != checkpoint_digest {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }
        let checkpoint =
            PaperExecutionCheckpoint::decode(configuration.clone(), bytes, maximum_bytes)?;
        if checkpoint.reconciliation_required() {
            return Err(PaperCheckpointRepositoryError::QuarantinedCheckpoint);
        }
        if checkpoint.recovery_digest()? != checkpoint_digest {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }
        let snapshot = crate::PaperExecutionSnapshot::from_state(
            checkpoint.configuration_digest(),
            &configuration,
            checkpoint.sequence(),
            checkpoint.reconciliation_required(),
            &checkpoint.orders,
            &checkpoint.fills,
            &checkpoint.archived_orders,
            &checkpoint.archived_fills,
            &checkpoint.ledger,
        );
        Ok(Self {
            encoded,
            snapshot,
            checkpoint_digest,
            ledger: checkpoint.ledger,
            permits_history_genesis: checkpoint.durable_sequence == 0,
        })
    }
    pub const fn snapshot(&self) -> &crate::PaperExecutionSnapshot {
        &self.snapshot
    }
    pub const fn checkpoint_digest(&self) -> [u8; 32] {
        self.checkpoint_digest
    }

    /// A zero original durability fence proves no archived order or fill could be purged.
    /// Otherwise a first publication needs retained canonical history; balances alone cannot
    /// establish complete transactions, even when the current checkpoint has no open positions.
    pub const fn permits_history_genesis(&self) -> bool {
        self.permits_history_genesis
    }

    /// Uses original settled cash and buy reservations from this exact checkpoint.
    pub fn available_cash(
        &self,
        account: AccountId,
        currency: Currency,
    ) -> Result<Money, PaperCheckpointRepositoryError> {
        self.ledger
            .available_cash(account, currency)
            .map_err(|error| PaperCheckpointError::Ledger(error).into())
    }

    /// Rejoins the exact originally admitted action plan without granting execution authority.
    pub fn verify_action_plan(
        &self,
        plan: Option<&market_squawk_data::CorporateActionPlan>,
    ) -> Result<(), PaperCheckpointRepositoryError> {
        match (
            self.ledger.requires_action_source_reopen(),
            self.snapshot.action_source_reference(),
            plan,
        ) {
            (false, None, None) => Ok(()),
            (true, Some(reference), Some(plan)) => self
                .ledger
                .verify_reopened_action_source(plan, reference)
                .map_err(|error| PaperCheckpointError::Ledger(error).into()),
            _ => Err(PaperCheckpointError::Ledger(
                crate::PaperLedgerError::InvalidActionEvidence,
            )
            .into()),
        }
    }
}

/// Immutable bytes issued only after exact original repository verification.
#[derive(Debug)]
pub struct PaperCheckpointBackup {
    manifest: Vec<u8>,
    checkpoint: Vec<u8>,
    configuration_history: Vec<u8>,
    source_reference: Option<Vec<u8>>,
    sequence: u64,
}
impl PaperCheckpointBackup {
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest
    }
    pub fn checkpoint_bytes(&self) -> &[u8] {
        &self.checkpoint
    }
    /// Exact bounded prior-policy objects required by retained paper audit configuration digests.
    pub fn configuration_history_bytes(&self) -> &[u8] {
        &self.configuration_history
    }
    pub fn action_source_reference(&self) -> Option<&[u8]> {
        self.source_reference.as_deref()
    }
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Holds the original cross-process writer lock, including an exactly empty repository.
#[derive(Debug)]
pub struct PaperCheckpointBackupLease {
    directory: Dir,
    maximum_bytes: NonZeroUsize,
    export: Option<PaperCheckpointBackup>,
    _writer_lock: std::fs::File,
}
impl PaperCheckpointBackupLease {
    pub fn checkpoint(&self) -> Option<&PaperCheckpointBackup> {
        self.export.as_ref()
    }
    /// Rejects a dirty marker, replacement, removed object, or changed exact current pointer.
    pub fn revalidate(&self) -> Result<(), PaperCheckpointRepositoryError> {
        validate_repository_writer(
            &self.directory,
            REPOSITORY_LOCK_PATH,
            &self._writer_lock,
        )?;
        reject_unclean_run(&self.directory)?;
        let actual = read_backup(&self.directory, self.maximum_bytes.get())?;
        match (&self.export, actual) {
            (None, None) => Ok(()),
            (Some(a), Some(b)) if a.manifest == b.manifest && a.checkpoint == b.checkpoint
                && a.configuration_history == b.configuration_history => {
                Ok(())
            }
            _ => Err(PaperCheckpointRepositoryError::AuthorityChanged),
        }
    }
}
impl PaperCheckpointRepository {
    /// Retains stopped persistence authority without deriving configuration from a current route.
    /// An active runtime holds these same writer locks; dirty or partial state is never exported.
    pub fn retain_stopped_backup(
        root: &ArtifactRoot,
        maximum_bytes: NonZeroUsize,
    ) -> Result<PaperCheckpointBackupLease, PaperCheckpointRepositoryError> {
        let directory = root.try_clone_directory()?;
        let writer_lock = acquire_repository_writer(&directory)?;
        reject_unclean_run(&directory)?;
        let export = read_backup(&directory, maximum_bytes.get())?;
        Ok(PaperCheckpointBackupLease {
            directory,
            maximum_bytes,
            export,
            _writer_lock: writer_lock,
        })
    }
    /// Opens only a genuine stopped repository using its original validated configuration.
    /// This does not start execution or replace the source-action reopen barrier.
    pub fn open_stopped(
        root: ArtifactRoot,
        maximum_bytes: NonZeroUsize,
    ) -> Result<Option<Self>, PaperCheckpointRepositoryError> {
        let directory = root.try_clone_directory()?;
        let writer_lock = acquire_repository_writer(&directory)?;
        reject_unclean_run(&directory)?;
        let Some(manifest) = read_original_manifest(&directory, maximum_bytes.get())? else {
            return Ok(None);
        };
        let config = manifest.configuration.to_config()?;
        read_backup(&directory, maximum_bytes.get())?
            .ok_or(PaperCheckpointRepositoryError::PartialState)?;
        let recovered = read_current_manifest(&directory, &config, maximum_bytes.get())?
            .ok_or(PaperCheckpointRepositoryError::PartialState)?;
        require_stopped(&recovered.checkpoint)?;
        Ok(Some(Self {
            root,
            config,
            maximum_bytes,
            repository_id: recovered.repository_id,
            generation: recovered.generation.get(),
            configuration_history: recovered.configuration_history,
            recovery: Some(PaperCheckpointRecovery {
                checkpoint: recovered.checkpoint,
                accounts: recovered.accounts,
            }),
            dirty_authority: None,
            _writer_lock: writer_lock,
        }))
    }
    pub const fn original_config(&self) -> &PaperExecutionConfig {
        &self.config
    }

    /// Consumes clean stopped authority and admits one genuinely sourced later session.
    ///
    /// The caller owns source-calendar admission. Every financial field and original DAY expiry
    /// stays unchanged. An error returns no writer; reopening selects the complete old or new
    /// manifest. Historical policies remain immutable audit dependencies through backup/restore.
    pub fn advance_session(
        self,
        admitted: PaperVenueSessionCalendar,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        self.advance_session_with_mark_age(admitted, None)
    }

    /// Binds an actual configured venue to the stopped original account. Only its first binding
    /// may adopt the venue's existing mark-age policy; cash, fee, ledger and other limits remain.
    pub fn bind_execution_policy(
        self,
        requested: PaperExecutionConfig,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        let admitted = requested.input().session_policy.calendar()
            .ok_or(PaperCheckpointRepositoryError::InvalidSessionAdvance)?.clone();
        let dormant = self.config.input().session_policy.calendar().is_none();
        let mut comparable = requested.input().clone();
        comparable.session_policy = self.config.input().session_policy.clone();
        if dormant {
            comparable.maximum_mark_age_nanos = self.config.input().maximum_mark_age_nanos;
        }
        if &comparable != self.config.input() {
            return Err(PaperCheckpointRepositoryError::ConfigurationMismatch);
        }
        let mark_age = dormant.then_some(requested.input().maximum_mark_age_nanos);
        self.advance_session_with_mark_age(admitted, mark_age)
    }

    fn advance_session_with_mark_age(
        mut self,
        admitted: PaperVenueSessionCalendar,
        first_mark_age: Option<u64>,
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        let directory = self.root.try_clone_directory()?;
        validate_repository_writer(&directory, REPOSITORY_LOCK_PATH, &self._writer_lock)?;
        reject_unclean_run(&directory)?;
        if self.dirty_authority.is_some() {
            return Err(PaperCheckpointRepositoryError::UncleanShutdown);
        }
        self.validate_current_authority(&directory)?;
        let recovery = self
            .recovery
            .as_ref()
            .ok_or(PaperCheckpointRepositoryError::UnstabilizedCheckpoint)?;
        require_stopped(&recovery.checkpoint)?;
        let original = self.config.input().session_policy.calendar();
        let [next] = admitted.sessions() else {
            return Err(PaperCheckpointRepositoryError::InvalidSessionAdvance);
        };
        let mut input = self.config.input().clone();
        input.session_policy = match original {
            None => {
                // Strict checkpoint decode already proves this is the original unmoved cash
                // ledger, with no session, order, fill, mark or financial mutation authority.
                if recovery.checkpoint.sequence() != 0 {
                    return Err(PaperCheckpointRepositoryError::InvalidSessionAdvance);
                }
                if let Some(mark_age) = first_mark_age {
                    input.maximum_mark_age_nanos = mark_age;
                }
                PaperExecutionSessionPolicy::Venue(admitted)
            }
            Some(original) => {
                if original.venue_id() != admitted.venue_id()
                    || original.time_zone() != admitted.time_zone()
                    || original.ruleset_version() != admitted.ruleset_version()
                {
                    return Err(PaperCheckpointRepositoryError::InvalidSessionAdvance);
                }
                let last = original
                    .sessions()
                    .last()
                    .ok_or(PaperCheckpointRepositoryError::InvalidSessionAdvance)?;
                if last.opens_at() == next.opens_at()
                    && last.closes_at_exclusive() == next.closes_at_exclusive()
                {
                    return Ok(self);
                }
                if next.opens_at() < last.closes_at_exclusive() {
                    return Err(PaperCheckpointRepositoryError::InvalidSessionAdvance);
                }
                // DAY admission used accepted_at to resolve its exact original calendar interval. Keep
                // that policy for every retained order, including archives. Claims and source replay use
                // their separate original source reference, never these simulator calendar session IDs.
                let mut required = std::collections::BTreeSet::new();
                for order in recovery
                    .checkpoint
                    .orders
                    .values()
                    .chain(recovery.checkpoint.archived_orders.values())
                    .filter(|order| order.time_in_force == market_squawk_domain::TimeInForce::Day)
                {
                    let index = original
                        .sessions()
                        .iter()
                        .position(|session| {
                            session.opens_at() <= order.accepted_at
                                && order.accepted_at < session.closes_at_exclusive()
                        })
                        .ok_or(PaperCheckpointRepositoryError::InvalidSessionAdvance)?;
                    if order.expires_at >= original.sessions()[index].closes_at_exclusive() {
                        return Err(PaperCheckpointRepositoryError::InvalidSessionAdvance);
                    }
                    required.insert(index);
                }
                if required.len() >= crate::MAX_PAPER_VENUE_SESSIONS {
                    return Err(PaperCheckpointRepositoryError::SessionRetentionCapacity);
                }
                let mut sessions = Vec::new();
                sessions
                    .try_reserve_exact(required.len() + 1)
                    .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
                sessions.extend(
                    required
                        .into_iter()
                        .map(|index| original.sessions()[index].clone()),
                );
                sessions.push(next.clone());
                PaperExecutionSessionPolicy::Venue(
                    PaperVenueSessionCalendar::try_new(
                        original.calendar_id().clone(),
                        original.ruleset_version(),
                        original.venue_id().clone(),
                        original.time_zone(),
                        sessions,
                    )
                    .map_err(|_| PaperCheckpointRepositoryError::InvalidSessionAdvance)?,
                )
            }
        };
        let configuration = PaperExecutionConfig::try_new(input)
            .map_err(|_| PaperCheckpointRepositoryError::ConfigurationMismatch)?;
        let history_object = ConfigurationHistoryWire {
            configuration_digest: self.config.digest(),
            configuration: ConfigWire::from_config(&self.config),
            previous: self.configuration_history,
        };
        let bytes = encode_bounded(&history_object, self.maximum_bytes.get())?;
        let history_digest = Sha256::digest(&bytes).into();
        let mut history = read_configuration_history(
            &directory,
            self.configuration_history,
            self.maximum_bytes.get(),
        )?;
        history
            .objects
            .try_reserve(1)
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        history.objects.insert(0, bytes.clone());
        // Check the exact backup envelope bound before any durable mutation. Append-only audit
        // policy dependencies cannot be pruned merely to admit another trading day.
        encode_bounded(&history, self.maximum_bytes.get())?;
        let recovery = self
            .recovery
            .take()
            .ok_or(PaperCheckpointRepositoryError::UnstabilizedCheckpoint)?;
        let mut checkpoint = recovery.checkpoint;
        checkpoint.configuration_digest = configuration.digest();
        let mut replay = Vec::new();
        replay
            .try_reserve_exact(recovery.accounts.len())
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        replay.extend(recovery.accounts.iter().map(|account| {
            PaperAccountReplaySnapshot::from_reconciled_state(
                account.state.clone(),
                account.idempotency.clone(),
            )
        }));
        publish_history_object(&self.root, &directory, &bytes, self.maximum_bytes.get())?;
        self.persist_with_configuration(
            &checkpoint,
            &replay,
            &configuration,
            history_digest,
            |_| Ok(()),
        )?;
        let verified = read_current_manifest(&directory, &configuration, self.maximum_bytes.get())?
            .ok_or(PaperCheckpointRepositoryError::PartialState)?;
        checkpoint.bind_current_manifest(self.repository_id, verified.generation);
        if verified.repository_id != self.repository_id
            || verified.generation.get() != self.generation
            || verified.checkpoint != checkpoint
            || verified.accounts != recovery.accounts
            || verified.configuration_history != history_digest
        {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }
        self.config = configuration;
        self.configuration_history = history_digest;
        self.recovery = Some(PaperCheckpointRecovery {
            checkpoint: verified.checkpoint,
            accounts: verified.accounts,
        });
        Ok(self)
    }

    /// Validates the stopped recovery payload and extracts only its inert original source recipe.
    /// Consumers must physically reopen that source before completing workspace restore.
    pub fn backup_source_reference(
        maximum_bytes: NonZeroUsize,
        manifest_bytes: &[u8],
        checkpoint_bytes: &[u8],
        configuration_history_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, PaperCheckpointRepositoryError> {
        if manifest_bytes.is_empty()
            || manifest_bytes.len() > maximum_bytes.get()
            || checkpoint_bytes.is_empty()
            || checkpoint_bytes.len() > maximum_bytes.get()
        {
            return Err(PaperCheckpointRepositoryError::InvalidManifest);
        }
        let manifest: CurrentManifestWire = serde_json::from_slice(manifest_bytes)
            .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
        let config = manifest.configuration.to_config()?;
        decode_configuration_history(
            manifest.configuration_history,
            configuration_history_bytes,
            maximum_bytes.get(),
        )?;
        require_stopped(&PaperExecutionCheckpoint::decode(
            config.clone(),
            checkpoint_bytes,
            maximum_bytes.get(),
        )?)?;
        let original =
            decode_manifest_content(&config, manifest, checkpoint_bytes, maximum_bytes.get())?;
        require_stopped(&original.checkpoint)?;
        Ok(original.checkpoint.ledger.action_source_reference())
    }
    /// Decodes through the same owner validators before publishing into a fresh repository.
    /// Exact original manifest identity, replay tombstones, and checkpoint bytes are preserved.
    pub fn restore_fresh(
        root: ArtifactRoot,
        maximum_bytes: NonZeroUsize,
        manifest_bytes: &[u8],
        checkpoint_bytes: &[u8],
        configuration_history_bytes: &[u8],
    ) -> Result<Self, PaperCheckpointRepositoryError> {
        if manifest_bytes.is_empty()
            || manifest_bytes.len() > maximum_bytes.get()
            || checkpoint_bytes.is_empty()
            || checkpoint_bytes.len() > maximum_bytes.get()
        {
            return Err(PaperCheckpointRepositoryError::InvalidManifest);
        }
        Self::backup_source_reference(
            maximum_bytes,
            manifest_bytes,
            checkpoint_bytes,
            configuration_history_bytes,
        )?;
        let manifest: CurrentManifestWire = serde_json::from_slice(manifest_bytes)
            .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
        let history = decode_configuration_history(
            manifest.configuration_history,
            configuration_history_bytes,
            maximum_bytes.get(),
        )?;
        let config = manifest.configuration.to_config()?;
        let original =
            decode_manifest_content(&config, manifest, checkpoint_bytes, maximum_bytes.get())?;
        require_stopped(&original.checkpoint)?;
        let original_identity = (original.repository_id, original.generation);
        drop(original);
        let mut repository = Self::try_new(root, config, maximum_bytes)?;
        if repository.generation != 0 || repository.recovery.is_some() {
            return Err(PaperCheckpointRepositoryError::PartialState);
        }
        let directory = repository.root.try_clone_directory()?;
        for bytes in history.objects.iter().rev() {
            publish_history_object(&repository.root, &directory, bytes, maximum_bytes.get())?;
        }
        let manifest: CurrentManifestWire = serde_json::from_slice(manifest_bytes)
            .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
        let artifact = Path::new(&manifest.artifact_reference);
        drop(repository.root.resolve(artifact)?);
        let parent = artifact
            .parent()
            .ok_or(PaperCheckpointRepositoryError::InvalidManifest)?;
        directory
            .create_dir_all(parent)
            .map_err(|e| io_error("create fresh paper checkpoint namespace", e))?;
        let staging_name = format!(
            "{}/stage-{}-{}-{}.tmp",
            parent.display(),
            hex_bytes(&manifest.repository_id)?,
            manifest.generation,
            random_hex()?
        );
        let staging_path = Path::new(&staging_name);
        drop(repository.root.resolve(staging_path)?);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut file = directory
            .open_with(staging_path, &options)
            .map_err(|e| io_error("create restored checkpoint stage", e))?;
        let mut guard = StagingGuard::new(&directory, staging_path);
        file.write_all(checkpoint_bytes)
            .map_err(|e| io_error("write restored checkpoint", e))?;
        file.sync_all()
            .map_err(|e| io_error("synchronize restored checkpoint", e))?;
        drop(file);
        if publish_new_staged_file(
            &repository.root,
            &directory,
            staging_path,
            artifact,
            &mut guard,
        )? != NewFilePublication::Published
        {
            return Err(PaperCheckpointRepositoryError::ContentConflict);
        }
        synchronize_publication_directories(&directory, artifact)?;
        let actual = read_bounded_regular(&directory, artifact, maximum_bytes.get())?;
        if actual != checkpoint_bytes {
            return Err(PaperCheckpointRepositoryError::VerificationFailed);
        }
        publish_current_manifest(
            &repository.root,
            &directory,
            &manifest,
            &hex_bytes(&manifest.repository_id)?,
            manifest.generation,
            maximum_bytes.get(),
        )?;
        repository.repository_id = original_identity.0;
        repository.generation = original_identity.1.get();
        let recovered = read_current_manifest(&directory, &repository.config, maximum_bytes.get())?
            .ok_or(PaperCheckpointRepositoryError::VerificationFailed)?;
        require_stopped(&recovered.checkpoint)?;
        repository.configuration_history = recovered.configuration_history;
        repository.recovery = Some(PaperCheckpointRecovery {
            checkpoint: recovered.checkpoint,
            accounts: recovered.accounts,
        });
        Ok(repository)
    }
}
fn require_stopped(
    checkpoint: &PaperExecutionCheckpoint,
) -> Result<(), PaperCheckpointRepositoryError> {
    if !checkpoint.complete()
        || checkpoint.has_nonterminal_orders()
        || checkpoint.reconciliation_required()
        || checkpoint.durable_sequence != checkpoint.sequence()
    {
        Err(PaperCheckpointRepositoryError::UnstabilizedCheckpoint)
    } else {
        Ok(())
    }
}

// Each node preserves one exact audit policy, not a growing copy of all prior policies.
// Zero is the empty chain. The current manifest commits the head; each immutable node commits
// its predecessor. Current paper audits are append-only, so none of these nodes can be pruned.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigurationHistoryWire {
    configuration_digest: [u8; 32],
    configuration: ConfigWire,
    previous: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfigurationHistoryBackup {
    objects: Vec<Vec<u8>>,
}

fn encode_bounded(
    value: &impl Serialize,
    maximum: usize,
) -> Result<Vec<u8>, PaperCheckpointRepositoryError> {
    let mut writer = BoundedRepositoryWriter::new(maximum)?;
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| PaperCheckpointRepositoryError::ConfigurationHistoryCapacity)?;
    Ok(writer.into_inner())
}

fn history_reference(digest: [u8; 32]) -> Result<String, PaperCheckpointRepositoryError> {
    let hex = hex_bytes(&digest)?;
    Ok(format!(
        "{CHECKPOINT_OBJECT_ROOT}/{}/{}.json",
        &hex[..2],
        hex
    ))
}

fn decode_history_object(
    expected: [u8; 32],
    bytes: &[u8],
) -> Result<ConfigurationHistoryWire, PaperCheckpointRepositoryError> {
    if expected == [0; 32] || <[u8; 32]>::from(Sha256::digest(bytes)) != expected {
        return Err(PaperCheckpointRepositoryError::VerificationFailed);
    }
    let object: ConfigurationHistoryWire =
        serde_json::from_slice(bytes).map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    if object.configuration.to_config()?.digest() != object.configuration_digest {
        return Err(PaperCheckpointRepositoryError::ConfigurationMismatch);
    }
    Ok(object)
}

pub(super) fn read_configuration_history(
    directory: &Dir,
    mut head: [u8; 32],
    maximum: usize,
) -> Result<ConfigurationHistoryBackup, PaperCheckpointRepositoryError> {
    let mut history = ConfigurationHistoryBackup {
        objects: Vec::new(),
    };
    let mut remaining = maximum;
    let mut visited = std::collections::BTreeSet::new();
    while head != [0; 32] {
        if !visited.insert(head) {
            return Err(PaperCheckpointRepositoryError::InvalidManifest);
        }
        if remaining == 0 {
            return Err(PaperCheckpointRepositoryError::ConfigurationHistoryCapacity);
        }
        let bytes =
            read_bounded_regular(directory, Path::new(&history_reference(head)?), remaining)?;
        head = decode_history_object(head, &bytes)?.previous;
        remaining = remaining
            .checked_sub(bytes.len())
            .ok_or(PaperCheckpointRepositoryError::ConfigurationHistoryCapacity)?;
        history
            .objects
            .try_reserve(1)
            .map_err(|_| PaperCheckpointRepositoryError::Allocation)?;
        history.objects.push(bytes);
    }
    // The same bound applies to exact export, so a readable chain is always representable.
    encode_bounded(&history, maximum)?;
    Ok(history)
}

fn decode_configuration_history(
    mut head: [u8; 32],
    bytes: &[u8],
    maximum: usize,
) -> Result<ConfigurationHistoryBackup, PaperCheckpointRepositoryError> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(PaperCheckpointRepositoryError::ConfigurationHistoryCapacity);
    }
    let history: ConfigurationHistoryBackup =
        serde_json::from_slice(bytes).map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    let mut visited = std::collections::BTreeSet::new();
    for bytes in &history.objects {
        if !visited.insert(head) {
            return Err(PaperCheckpointRepositoryError::InvalidManifest);
        }
        head = decode_history_object(head, bytes)?.previous;
    }
    if head != [0; 32] {
        return Err(PaperCheckpointRepositoryError::InvalidManifest);
    }
    Ok(history)
}

fn publish_history_object(
    root: &ArtifactRoot,
    directory: &Dir,
    bytes: &[u8],
    maximum: usize,
) -> Result<(), PaperCheckpointRepositoryError> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(PaperCheckpointRepositoryError::ConfigurationHistoryCapacity);
    }
    let digest = Sha256::digest(bytes).into();
    decode_history_object(digest, bytes)?;
    let reference = history_reference(digest)?;
    let path = Path::new(&reference);
    drop(root.resolve(path)?);
    let parent = path
        .parent()
        .ok_or(PaperCheckpointRepositoryError::InvalidManifest)?;
    directory
        .create_dir_all(parent)
        .map_err(|error| io_error("create paper policy history shard", error))?;
    let staging = format!(
        "{}/stage-{}-1-{}.tmp",
        parent.display(),
        hex_bytes(&digest)?,
        random_hex()?
    );
    let staging_path = Path::new(&staging);
    drop(root.resolve(staging_path)?);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let mut file = directory
        .open_with(staging_path, &options)
        .map_err(|error| io_error("create paper policy history stage", error))?;
    let mut guard = StagingGuard::new(directory, staging_path);
    file.write_all(bytes)
        .map_err(|error| io_error("write paper policy history", error))?;
    file.sync_all()
        .map_err(|error| io_error("synchronize paper policy history", error))?;
    drop(file);
    publish_new_staged_file(root, directory, staging_path, path, &mut guard)?;
    synchronize_publication_directories(directory, path)?;
    if read_bounded_regular(directory, path, maximum)? != bytes {
        return Err(PaperCheckpointRepositoryError::ContentConflict);
    }
    Ok(())
}
fn read_original_manifest(
    directory: &Dir,
    maximum: usize,
) -> Result<Option<CurrentManifestWire>, PaperCheckpointRepositoryError> {
    match directory.symlink_metadata(CURRENT_MANIFEST_PATH) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if checkpoint_namespace_contains_only_writer_lock(directory)? {
                return Ok(None);
            }
            return Err(PaperCheckpointRepositoryError::PartialState);
        }
        Err(e) => return Err(io_error("inspect original paper manifest", e)),
        Ok(m) if !m.file_type().is_file() => {
            return Err(PaperCheckpointRepositoryError::UnsafeArtifact);
        }
        Ok(_) => {}
    }
    let bytes = read_bounded_regular(directory, Path::new(CURRENT_MANIFEST_PATH), maximum)?;
    let manifest: CurrentManifestWire =
        serde_json::from_slice(&bytes).map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(PaperCheckpointRepositoryError::InvalidManifest);
    }
    Ok(Some(manifest))
}
fn read_backup(
    directory: &Dir,
    maximum: usize,
) -> Result<Option<PaperCheckpointBackup>, PaperCheckpointRepositoryError> {
    let Some(manifest) = read_original_manifest(directory, maximum)? else {
        return Ok(None);
    };
    let config = manifest.configuration.to_config()?;
    let artifact_reference = manifest.artifact_reference.clone();
    let history = read_configuration_history(directory, manifest.configuration_history, maximum)?;
    let configuration_history = encode_bounded(&history, maximum)?;
    drop(manifest);
    let recovered = read_current_manifest(directory, &config, maximum)?
        .ok_or(PaperCheckpointRepositoryError::PartialState)?;
    require_stopped(&recovered.checkpoint)?;
    let identity = (recovered.repository_id, recovered.generation);
    drop(recovered);
    let checkpoint = read_bounded_regular(directory, Path::new(&artifact_reference), maximum)?;
    require_stopped(&PaperExecutionCheckpoint::decode(
        config.clone(),
        &checkpoint,
        maximum,
    )?)?;
    let manifest = read_bounded_regular(directory, Path::new(CURRENT_MANIFEST_PATH), maximum)?;
    // Compare the exact second reads through the owner again; no pointer replacement can be hidden.
    let wire: CurrentManifestWire = serde_json::from_slice(&manifest)
        .map_err(PaperCheckpointRepositoryError::ManifestEncoding)?;
    decode_configuration_history(wire.configuration_history, &configuration_history, maximum)?;
    let verified = decode_manifest_content(&config, wire, &checkpoint, maximum)?;
    require_stopped(&verified.checkpoint)?;
    if (verified.repository_id, verified.generation) != identity {
        return Err(PaperCheckpointRepositoryError::AuthorityChanged);
    }
    Ok(Some(PaperCheckpointBackup {
        configuration_history,
        manifest,
        checkpoint,
        source_reference: verified.checkpoint.ledger.action_source_reference(),
        sequence: verified.checkpoint.sequence(),
    }))
}
