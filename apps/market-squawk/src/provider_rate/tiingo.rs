//! Tiingo quota, history, and schema state inside the shared provider-rate SQLite authority.

use std::fmt;
use std::sync::Mutex;

use market_squawk_adapter_tiingo::{
    TIINGO_APPLICATION_BYTES_PER_MONTH, TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH,
    TIINGO_PROVIDER_BYTES_PER_MONTH, TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH,
    TiingoCompletedResponseDisposition, TiingoEndpointFamily, TiingoHistoryCheckpointReceipt,
    TiingoHistoryPlan, TiingoProviderAdmissionDecision, TiingoProviderAdmissionRequest,
    TiingoProviderAuthority, TiingoProviderAuthorityError, TiingoProviderAuthorityInstallation,
    TiingoProviderAuthorityRequirements, TiingoProviderPermit, TiingoQuotaError, TiingoQuotaLedger,
    TiingoQuotaPermit, TiingoQuotaSnapshot, TiingoQuotaWindows, TiingoRateLimitDisposition,
    TiingoResponseSettlement, TiingoSchemaChange, TiingoSchemaChangeReason,
    TiingoSchemaCircuitState, TiingoSealedHistoryPage, TiingoTicker,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    BudgetUnavailableReason, ProviderRateAuthority, ProviderRateDeclaration,
    ProviderRateExtensionKey, ProviderRateExtensionRevision, ProviderRateExtensionState,
    ProviderRateStoreError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const EXTENSION_ID: &str = "tiingo-provider-authority";
const EXTENSION_SCHEMA_ID: &str = "tiingo-provider-authority-state-v1";
const AUTHORITY_GENERATION: &str = "tiingo-provider-authority-v1";
const DURABLE_STORE_GENERATION: &str = "provider-rate-sqlite-schema-v3";
const STATE_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_RETRY_AFTER_BYTES: usize = 128;
const HOUR_NANOS: i64 = 3_600_000_000_000;
const DAY_NANOS: i64 = 86_400_000_000_000;
// Provider reset coordinates are unpublished. A full 32 elapsed days is deliberately stricter
// than any Gregorian calendar month and never resets monthly use early.
const CONSERVATIVE_MONTH_NANOS: i64 = 2_764_800_000_000_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableTiingoState {
    schema_version: u16,
    source_id: String,
    source_contract_revision: String,
    native_contract_revision: String,
    entitlement_generation: String,
    installed_at: Timestamp,
    authority_evidence_sha256: [u8; 32],
    quota: TiingoQuotaSnapshot,
    quota_overage_until: Option<Timestamp>,
    history: Option<DurableHistoryCheckpoint>,
    schema_change: Option<DurableSchemaChange>,
    last_rate_limit: Option<DurableRateLimit>,
    next_permit: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableHistoryCheckpoint {
    ticker: String,
    start_date: CalendarDate,
    end_date: CalendarDate,
    plan_identity_sha256: [u8; 32],
    maximum_response_bytes: u64,
    page_count: u32,
    next_page_index: u32,
    predecessor_page_sha256: Option<[u8; 32]>,
    authority_receipt_sha256: [u8; 32],
    checkpointed_at: Timestamp,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableSchemaChange {
    endpoint: u8,
    reason: u8,
    response_sha256: [u8; 32],
    observed_at: Timestamp,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableRateLimit {
    retry_after: Option<Vec<u8>>,
    jitter_sample_basis_points: u16,
    observed_at: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InstalledTiingoAuthority {
    installation: TiingoProviderAuthorityInstallation,
    source_id: String,
    source_contract_revision: String,
    native_contract_revision: String,
    entitlement_generation: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingTiingoPermit {
    provider: TiingoProviderPermit,
    quota: TiingoQuotaPermit,
}

/// Production Tiingo authority over the same serialized provider-rate store and process run.
pub(crate) struct DurableTiingoProviderAuthority {
    provider_rate: ProviderRateAuthority,
    extension_key: ProviderRateExtensionKey,
    initial_observed_at: Timestamp,
    initial_windows: TiingoQuotaWindows,
    transition_gate: Mutex<()>,
    installed: Mutex<Option<InstalledTiingoAuthority>>,
    pending: Mutex<Option<PendingTiingoPermit>>,
}

impl DurableTiingoProviderAuthority {
    /// Binds Tiingo state to the exact generic declaration before the adapter registers and opens
    /// its shared budget. The supplied initial windows are used only when no durable row exists.
    pub(crate) fn try_new(
        provider_rate: ProviderRateAuthority,
        declaration: &ProviderRateDeclaration,
        initial_observed_at: Timestamp,
        initial_windows: TiingoQuotaWindows,
    ) -> Result<Self, TiingoProviderAuthorityError> {
        if initial_observed_at.unix_nanos() < 0
            || initial_windows.hour_resets_at() <= initial_observed_at
            || initial_windows.day_resets_at() < initial_windows.hour_resets_at()
            || initial_windows.month_resets_at() < initial_windows.day_resets_at()
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        let extension_key = ProviderRateExtensionKey::try_from_declaration(
            declaration,
            identifier(EXTENSION_ID)?,
            identifier(EXTENSION_SCHEMA_ID)?,
        )
        .map_err(|_| TiingoProviderAuthorityError::InvalidReceipt)?;
        Ok(Self {
            provider_rate,
            extension_key,
            initial_observed_at,
            initial_windows,
            transition_gate: Mutex::new(()),
            installed: Mutex::new(None),
            pending: Mutex::new(None),
        })
    }

    fn installed(&self) -> Result<InstalledTiingoAuthority, TiingoProviderAuthorityError> {
        self.installed
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
            .clone()
            .ok_or(TiingoProviderAuthorityError::Conflict)
    }

    fn load_state(
        &self,
    ) -> Result<
        Option<(ProviderRateExtensionState, DurableTiingoState)>,
        TiingoProviderAuthorityError,
    > {
        self.provider_rate
            .load_extension(&self.extension_key)
            .map_err(map_store_error)?
            .map(|stored| {
                if stored.key() != &self.extension_key {
                    return Err(TiingoProviderAuthorityError::Corrupt);
                }
                let state = decode_state(stored.bytes())?;
                Ok((stored, state))
            })
            .transpose()
    }

    fn replace_state(
        &self,
        expected: Option<ProviderRateExtensionRevision>,
        state: &DurableTiingoState,
    ) -> Result<ProviderRateExtensionState, TiingoProviderAuthorityError> {
        validate_state(state)?;
        let bytes = encode_state(state)?;
        if bytes.len() > ProviderRateExtensionState::MAXIMUM_BYTES {
            return Err(TiingoProviderAuthorityError::Unavailable);
        }
        let stored = self
            .provider_rate
            .compare_exchange_extension(&self.extension_key, expected, &bytes)
            .map_err(map_store_error)?;
        if stored.key() != &self.extension_key || stored.bytes() != bytes {
            return Err(TiingoProviderAuthorityError::Corrupt);
        }
        if decode_state(stored.bytes())? != *state {
            return Err(TiingoProviderAuthorityError::Corrupt);
        }
        Ok(stored)
    }

    fn installation_from_state(
        &self,
        requirements: &TiingoProviderAuthorityRequirements,
        state: &DurableTiingoState,
    ) -> Result<TiingoProviderAuthorityInstallation, TiingoProviderAuthorityError> {
        TiingoProviderAuthorityInstallation::try_new(
            requirements,
            identifier(AUTHORITY_GENERATION)?,
            identifier(DURABLE_STORE_GENERATION)?,
            sha256_evidence(state.authority_evidence_sha256)?,
            state.installed_at,
        )
    }
}

impl fmt::Debug for DurableTiingoProviderAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableTiingoProviderAuthority")
            .field("extension_key", &self.extension_key)
            .finish_non_exhaustive()
    }
}

impl TiingoProviderAuthority for DurableTiingoProviderAuthority {
    fn validate_requirements(
        &self,
        requirements: &TiingoProviderAuthorityRequirements,
    ) -> Result<TiingoProviderAuthorityInstallation, TiingoProviderAuthorityError> {
        let _transition = self
            .transition_gate
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        validate_requirements(requirements, &self.extension_key)?;
        if self
            .pending
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
            .is_some()
        {
            return Err(TiingoProviderAuthorityError::Conflict);
        }

        let (stored, mut state) = match self.load_state()? {
            Some((stored, state)) => (stored, state),
            None => {
                let admitted_at = self
                    .provider_rate
                    .extension_clock_timestamp()
                    .map_err(map_store_error)?;
                if admitted_at < self.initial_observed_at {
                    return Err(TiingoProviderAuthorityError::Unavailable);
                }
                let state = initial_state(
                    requirements,
                    &self.extension_key,
                    admitted_at,
                    self.initial_windows,
                )?;
                let stored = self.replace_state(None, &state)?;
                (stored, state)
            }
        };
        validate_state_for_requirements(&state, requirements, &self.extension_key)?;

        let mut quota = TiingoQuotaLedger::try_restore(state.quota.clone())
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        if quota
            .reconcile_incomplete_response()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
        {
            state.quota = quota.snapshot().clone();
            self.replace_state(Some(stored.revision()), &state)?;
        }

        let installation = self.installation_from_state(requirements, &state)?;
        let admitted = InstalledTiingoAuthority {
            installation: installation.clone(),
            source_id: state.source_id.clone(),
            source_contract_revision: state.source_contract_revision.clone(),
            native_contract_revision: state.native_contract_revision.clone(),
            entitlement_generation: state.entitlement_generation.clone(),
        };
        let mut installed = self
            .installed
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        if installed
            .as_ref()
            .is_some_and(|current| current != &admitted)
        {
            return Err(TiingoProviderAuthorityError::Conflict);
        }
        *installed = Some(admitted);
        Ok(installation)
    }

    fn prepare_history_plan(
        &self,
        plan: &TiingoHistoryPlan,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError> {
        let _transition = self
            .transition_gate
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let installed = self.installed()?;
        if self
            .pending
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
            .is_some()
        {
            return Err(TiingoProviderAuthorityError::Conflict);
        }
        let (stored, mut state) = self
            .load_state()?
            .ok_or(TiingoProviderAuthorityError::Conflict)?;
        validate_state_for_installation(&state, &installed)?;
        if let Some(history) = &state.history {
            let existing_plan = history.plan()?;
            if existing_plan == *plan {
                return history.receipt(&installed.installation);
            }
            if history.next_page_index < history.page_count {
                return Err(TiingoProviderAuthorityError::Conflict);
            }
        }
        let now = self
            .provider_rate
            .extension_clock_timestamp()
            .map_err(map_store_error)?;
        let authority_receipt_sha256 = digest_fields(&[
            b"market-squawk/tiingo/history-plan-authority/v1",
            &plan.request_set_identity().bytes(),
            &state.authority_evidence_sha256,
        ])
        .bytes();
        let history = DurableHistoryCheckpoint::from_plan(plan, authority_receipt_sha256, now)?;
        let receipt = history.receipt(&installed.installation)?;
        state.history = Some(history);
        self.replace_state(Some(stored.revision()), &state)?;
        Ok(receipt)
    }

    fn checkpoint_history_page(
        &self,
        checkpoint: &TiingoHistoryCheckpointReceipt,
        page: &TiingoSealedHistoryPage,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError> {
        let _transition = self
            .transition_gate
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let installed = self.installed()?;
        if self
            .pending
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
            .is_some()
        {
            return Err(TiingoProviderAuthorityError::Conflict);
        }
        let (stored, mut state) = self
            .load_state()?
            .ok_or(TiingoProviderAuthorityError::Conflict)?;
        validate_state_for_installation(&state, &installed)?;
        let history = state
            .history
            .as_mut()
            .ok_or(TiingoProviderAuthorityError::Conflict)?;
        let plan = history.plan()?;
        if history.receipt(&installed.installation)? != *checkpoint
            || history.next_page_index >= history.page_count
        {
            return Err(TiingoProviderAuthorityError::Conflict);
        }
        let page_index = usize::try_from(history.next_page_index)
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        if plan.pages().get(page_index) != Some(page.request())
            || page.source_id().as_str() != installed.source_id
            || page
                .source_contract_revision()
                .as_source_identifier()
                .as_str()
                != installed.source_contract_revision
            || page.native_contract_revision().as_str() != installed.native_contract_revision
            || page.entitlement_generation().as_str() != installed.entitlement_generation
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        let next_index = history
            .next_page_index
            .checked_add(1)
            .ok_or(TiingoProviderAuthorityError::Corrupt)?;
        history.authority_receipt_sha256 = digest_fields(&[
            b"market-squawk/tiingo/history-page-authority/v1",
            &history.authority_receipt_sha256,
            &page.page_identity().bytes(),
            &next_index.to_be_bytes(),
        ])
        .bytes();
        history.next_page_index = next_index;
        history.predecessor_page_sha256 = Some(page.page_identity().bytes());
        history.checkpointed_at = self
            .provider_rate
            .extension_clock_timestamp()
            .map_err(map_store_error)?;
        let next = history.receipt(&installed.installation)?;
        self.replace_state(Some(stored.revision()), &state)?;
        Ok(next)
    }

    fn try_acquire(
        &self,
        request: &TiingoProviderAdmissionRequest,
    ) -> Result<TiingoProviderAdmissionDecision, TiingoProviderAuthorityError> {
        let _transition = self
            .transition_gate
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let installed = self.installed()?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        if pending.is_some() {
            return Err(TiingoProviderAuthorityError::Conflict);
        }
        let (mut stored, mut state) = self
            .load_state()?
            .ok_or(TiingoProviderAuthorityError::Conflict)?;
        validate_state_for_installation(&state, &installed)?;
        let now = self
            .provider_rate
            .extension_clock_timestamp()
            .map_err(map_store_error)?;
        let mut quota = TiingoQuotaLedger::try_restore(state.quota.clone())
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        if quota.snapshot().pending_response().is_some() {
            quota
                .reconcile_incomplete_response()
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
            state.quota = quota.snapshot().clone();
            stored = self.replace_state(Some(stored.revision()), &state)?;
        }
        if let Some(month_reset) = advance_quota_windows(&mut quota, now)? {
            if month_reset {
                state.quota_overage_until = None;
            }
            state.quota = quota.snapshot().clone();
            stored = self.replace_state(Some(stored.revision()), &state)?;
        }
        if state.quota_overage_until.is_some_and(|until| now < until) {
            return Ok(TiingoProviderAdmissionDecision::Unavailable(
                BudgetUnavailableReason::Disabled,
            ));
        }
        if let Some(change) = &state.schema_change {
            return Ok(TiingoProviderAdmissionDecision::SchemaCircuitOpen(
                change.to_adapter()?,
            ));
        }
        validate_history_request(&state, request, &installed.installation)?;
        let quota_permit = match quota
            .reserve(request.ticker().clone(), request.maximum_response_bytes())
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
        {
            Ok(permit) => permit,
            Err(admission) => return Ok(TiingoProviderAdmissionDecision::QuotaDenied(admission)),
        };
        let next_permit = state
            .next_permit
            .checked_add(1)
            .ok_or(TiingoProviderAuthorityError::Corrupt)?;
        let permit_identity = digest_fields(&[
            b"market-squawk/tiingo/provider-permit/v1",
            &stored.revision().digest().bytes(),
            &request.request_identity().bytes(),
            &next_permit.to_be_bytes(),
        ]);
        let provider_permit = TiingoProviderPermit::try_new(
            request.ticker().clone(),
            request.request_identity(),
            request.maximum_response_bytes(),
            identifier(AUTHORITY_GENERATION)?,
            installed.installation.installation_identity(),
            request
                .history_checkpoint()
                .map(TiingoHistoryCheckpointReceipt::receipt_identity),
            permit_identity,
            now,
        )?;
        state.quota = quota.snapshot().clone();
        state.next_permit = next_permit;
        self.replace_state(Some(stored.revision()), &state)?;
        *pending = Some(PendingTiingoPermit {
            provider: provider_permit.clone(),
            quota: quota_permit,
        });
        Ok(TiingoProviderAdmissionDecision::Ready(provider_permit))
    }

    fn settle_response(
        &self,
        permit: &TiingoProviderPermit,
        settlement: &TiingoResponseSettlement,
    ) -> Result<Option<TiingoRateLimitDisposition>, TiingoProviderAuthorityError> {
        let _transition = self
            .transition_gate
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let installed = self.installed()?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let active = pending
            .as_ref()
            .ok_or(TiingoProviderAuthorityError::Conflict)?
            .clone();
        if active.provider != *permit
            || permit.authority_generation().as_str() != AUTHORITY_GENERATION
            || permit.installation_identity() != installed.installation.installation_identity()
            || !valid_settlement(permit, settlement)
        {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        let (stored, mut state) = self
            .load_state()?
            .ok_or(TiingoProviderAuthorityError::Conflict)?;
        validate_state_for_installation(&state, &installed)?;
        let mut quota = TiingoQuotaLedger::try_restore(state.quota.clone())
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let quota_result = if settlement.was_not_dispatched() {
            quota.cancel_before_dispatch(&active.quota, permit.ticker())
        } else {
            quota.commit_response(
                &active.quota,
                permit.ticker(),
                settlement.charged_response_bytes(),
            )
        };
        match quota_result {
            Ok(()) => {}
            Err(TiingoQuotaError::ResponseExceededReservation) => {
                state.quota_overage_until = Some(quota.snapshot().windows().month_resets_at());
            }
            Err(_) => return Err(TiingoProviderAuthorityError::Corrupt),
        }
        state.quota = quota.snapshot().clone();
        let now = self
            .provider_rate
            .extension_clock_timestamp()
            .map_err(map_store_error)?;
        let mut rate_limited = false;
        if let Some(disposition) = settlement.complete_disposition() {
            match disposition {
                TiingoCompletedResponseDisposition::DecodedSuccess
                | TiingoCompletedResponseDisposition::ProviderRefusal
                | TiingoCompletedResponseDisposition::Rejected => {}
                TiingoCompletedResponseDisposition::ProviderRateLimited {
                    retry_after,
                    jitter_sample_basis_points,
                } => {
                    if retry_after
                        .as_ref()
                        .is_some_and(|bytes| bytes.len() > MAXIMUM_RETRY_AFTER_BYTES)
                        || *jitter_sample_basis_points > 10_000
                    {
                        return Err(TiingoProviderAuthorityError::InvalidReceipt);
                    }
                    state.last_rate_limit = Some(DurableRateLimit {
                        retry_after: retry_after.as_ref().map(|bytes| bytes.to_vec()),
                        jitter_sample_basis_points: *jitter_sample_basis_points,
                        observed_at: now,
                    });
                    rate_limited = true;
                }
                TiingoCompletedResponseDisposition::SchemaChanged {
                    contract_revision,
                    change,
                } => {
                    if contract_revision.as_str() != installed.native_contract_revision {
                        return Err(TiingoProviderAuthorityError::InvalidReceipt);
                    }
                    let durable = DurableSchemaChange::from_adapter(change);
                    if state
                        .schema_change
                        .as_ref()
                        .is_some_and(|current| current != &durable)
                    {
                        return Err(TiingoProviderAuthorityError::Conflict);
                    }
                    state.schema_change = Some(durable);
                }
            }
        }
        self.replace_state(Some(stored.revision()), &state)?;
        *pending = None;
        Ok(
            rate_limited.then_some(TiingoRateLimitDisposition::Unavailable(
                BudgetUnavailableReason::Disabled,
            )),
        )
    }

    fn schema_circuit_state(
        &self,
        contract_revision: &SourceIdentifier,
    ) -> Result<TiingoSchemaCircuitState, TiingoProviderAuthorityError> {
        let _transition = self
            .transition_gate
            .lock()
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        let installed = self.installed()?;
        if contract_revision.as_str() != installed.native_contract_revision {
            return Err(TiingoProviderAuthorityError::InvalidReceipt);
        }
        let (_stored, state) = self
            .load_state()?
            .ok_or(TiingoProviderAuthorityError::Conflict)?;
        validate_state_for_installation(&state, &installed)?;
        state.schema_change.map_or_else(
            || Ok(TiingoSchemaCircuitState::Closed),
            |change| Ok(TiingoSchemaCircuitState::Open(change.to_adapter()?)),
        )
    }
}

impl DurableHistoryCheckpoint {
    fn from_plan(
        plan: &TiingoHistoryPlan,
        authority_receipt_sha256: [u8; 32],
        checkpointed_at: Timestamp,
    ) -> Result<Self, TiingoProviderAuthorityError> {
        let (start_date, end_date) = plan.interval();
        Ok(Self {
            ticker: plan.ticker().as_str().to_owned(),
            start_date,
            end_date,
            plan_identity_sha256: plan.request_set_identity().bytes(),
            maximum_response_bytes: plan.maximum_response_bytes(),
            page_count: u32::try_from(plan.pages().len())
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?,
            next_page_index: 0,
            predecessor_page_sha256: None,
            authority_receipt_sha256,
            checkpointed_at,
        })
    }

    fn plan(&self) -> Result<TiingoHistoryPlan, TiingoProviderAuthorityError> {
        let plan = TiingoHistoryPlan::try_new(
            TiingoTicker::try_new(self.ticker.clone())
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?,
            self.start_date,
            self.end_date,
        )
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
        if plan.request_set_identity().bytes() != self.plan_identity_sha256
            || plan.maximum_response_bytes() != self.maximum_response_bytes
            || u32::try_from(plan.pages().len()).ok() != Some(self.page_count)
        {
            return Err(TiingoProviderAuthorityError::Corrupt);
        }
        Ok(plan)
    }

    fn receipt(
        &self,
        installation: &TiingoProviderAuthorityInstallation,
    ) -> Result<TiingoHistoryCheckpointReceipt, TiingoProviderAuthorityError> {
        TiingoHistoryCheckpointReceipt::try_new(
            &self.plan()?,
            self.next_page_index,
            self.predecessor_page_sha256
                .map(sha256_evidence)
                .transpose()?,
            identifier(AUTHORITY_GENERATION)?,
            installation.installation_identity(),
            sha256_evidence(self.authority_receipt_sha256)?,
            self.checkpointed_at,
        )
    }
}

impl DurableSchemaChange {
    fn from_adapter(change: &TiingoSchemaChange) -> Self {
        Self {
            endpoint: endpoint_id(change.endpoint()),
            reason: schema_reason_id(change.reason()),
            response_sha256: change.response_digest().bytes(),
            observed_at: change.observed_at(),
        }
    }

    fn to_adapter(&self) -> Result<TiingoSchemaChange, TiingoProviderAuthorityError> {
        TiingoSchemaChange::try_from_durable_evidence(
            endpoint_from_id(self.endpoint)?,
            schema_reason_from_id(self.reason)?,
            sha256_evidence(self.response_sha256)?,
            self.observed_at,
        )
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)
    }
}

fn initial_state(
    requirements: &TiingoProviderAuthorityRequirements,
    key: &ProviderRateExtensionKey,
    installed_at: Timestamp,
    windows: TiingoQuotaWindows,
) -> Result<DurableTiingoState, TiingoProviderAuthorityError> {
    let state = DurableTiingoState {
        schema_version: STATE_SCHEMA_VERSION,
        source_id: requirements.source_id().as_str().to_owned(),
        source_contract_revision: requirements
            .source_contract_revision()
            .as_source_identifier()
            .as_str()
            .to_owned(),
        native_contract_revision: requirements.native_contract_revision().as_str().to_owned(),
        entitlement_generation: requirements.entitlement_generation().as_str().to_owned(),
        installed_at,
        authority_evidence_sha256: requirements_evidence(requirements, key).bytes(),
        quota: TiingoQuotaLedger::new(windows).snapshot().clone(),
        quota_overage_until: None,
        history: None,
        schema_change: None,
        last_rate_limit: None,
        next_permit: 0,
    };
    validate_state(&state)?;
    Ok(state)
}

fn validate_requirements(
    requirements: &TiingoProviderAuthorityRequirements,
    key: &ProviderRateExtensionKey,
) -> Result<(), TiingoProviderAuthorityError> {
    requirements
        .provider_rate_declaration()
        .validate()
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
    if requirements
        .provider_rate_declaration()
        .declaration_digest()
        != key.declaration_digest()
        || requirements.provider_rate_declaration().policy_digest() != key.policy_digest()
        || requirements.provider_unique_symbols_per_month()
            != TIINGO_PROVIDER_UNIQUE_SYMBOLS_PER_MONTH
        || requirements.application_unique_symbols_per_month()
            != TIINGO_APPLICATION_UNIQUE_SYMBOLS_PER_MONTH
        || requirements.provider_bytes_per_month() != TIINGO_PROVIDER_BYTES_PER_MONTH
        || requirements.application_bytes_per_month() != TIINGO_APPLICATION_BYTES_PER_MONTH
        || key.extension_id().as_str() != EXTENSION_ID
        || key.schema_id().as_str() != EXTENSION_SCHEMA_ID
    {
        return Err(TiingoProviderAuthorityError::InvalidReceipt);
    }
    Ok(())
}

fn validate_state_for_requirements(
    state: &DurableTiingoState,
    requirements: &TiingoProviderAuthorityRequirements,
    key: &ProviderRateExtensionKey,
) -> Result<(), TiingoProviderAuthorityError> {
    validate_state(state)?;
    if state.source_id != requirements.source_id().as_str()
        || state.source_contract_revision
            != requirements
                .source_contract_revision()
                .as_source_identifier()
                .as_str()
        || state.native_contract_revision != requirements.native_contract_revision().as_str()
        || state.entitlement_generation != requirements.entitlement_generation().as_str()
        || state.authority_evidence_sha256 != requirements_evidence(requirements, key).bytes()
    {
        return Err(TiingoProviderAuthorityError::Conflict);
    }
    Ok(())
}

fn validate_state_for_installation(
    state: &DurableTiingoState,
    installed: &InstalledTiingoAuthority,
) -> Result<(), TiingoProviderAuthorityError> {
    validate_state(state)?;
    if state.source_id != installed.source_id
        || state.source_contract_revision != installed.source_contract_revision
        || state.native_contract_revision != installed.native_contract_revision
        || state.entitlement_generation != installed.entitlement_generation
        || state.installed_at != installed.installation.admitted_at()
        || sha256_evidence(state.authority_evidence_sha256)?
            != installed.installation.authority_evidence()
    {
        return Err(TiingoProviderAuthorityError::Conflict);
    }
    Ok(())
}

fn validate_state(state: &DurableTiingoState) -> Result<(), TiingoProviderAuthorityError> {
    if state.schema_version != STATE_SCHEMA_VERSION
        || state.source_id.is_empty()
        || state.source_contract_revision.is_empty()
        || state.native_contract_revision.is_empty()
        || state.entitlement_generation.is_empty()
        || state.installed_at.unix_nanos() < 0
        || state.authority_evidence_sha256 == [0; 32]
        || state
            .quota_overage_until
            .is_some_and(|until| until != state.quota.windows().month_resets_at())
    {
        return Err(TiingoProviderAuthorityError::Corrupt);
    }
    TiingoQuotaLedger::try_restore(state.quota.clone())
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
    if let Some(history) = &state.history {
        history.plan()?;
        if history.page_count == 0
            || history.next_page_index > history.page_count
            || (history.next_page_index == 0) != history.predecessor_page_sha256.is_none()
            || history.authority_receipt_sha256 == [0; 32]
            || history.checkpointed_at < state.installed_at
        {
            return Err(TiingoProviderAuthorityError::Corrupt);
        }
    }
    if let Some(change) = &state.schema_change {
        change.to_adapter()?;
    }
    if let Some(rate_limit) = &state.last_rate_limit
        && (rate_limit
            .retry_after
            .as_ref()
            .is_some_and(|bytes| bytes.len() > MAXIMUM_RETRY_AFTER_BYTES)
            || rate_limit.jitter_sample_basis_points > 10_000
            || rate_limit.observed_at < state.installed_at)
    {
        return Err(TiingoProviderAuthorityError::Corrupt);
    }
    Ok(())
}

fn validate_history_request(
    state: &DurableTiingoState,
    request: &TiingoProviderAdmissionRequest,
    installation: &TiingoProviderAuthorityInstallation,
) -> Result<(), TiingoProviderAuthorityError> {
    let Some(checkpoint) = request.history_checkpoint() else {
        return Ok(());
    };
    let history = state
        .history
        .as_ref()
        .ok_or(TiingoProviderAuthorityError::Conflict)?;
    if history.receipt(installation)? != *checkpoint
        || history.next_page_index >= history.page_count
    {
        return Err(TiingoProviderAuthorityError::Conflict);
    }
    let plan = history.plan()?;
    let index = usize::try_from(history.next_page_index)
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
    if plan
        .pages()
        .get(index)
        .map(market_squawk_adapter_tiingo::TiingoRequestSpec::request_identity)
        != Some(request.request_identity())
    {
        return Err(TiingoProviderAuthorityError::InvalidReceipt);
    }
    Ok(())
}

fn advance_quota_windows(
    quota: &mut TiingoQuotaLedger,
    observed_at: Timestamp,
) -> Result<Option<bool>, TiingoProviderAuthorityError> {
    let current = quota.snapshot().windows();
    if observed_at < current.hour_resets_at() {
        return Ok(None);
    }
    let month_reset = observed_at >= current.month_resets_at();
    let month = if month_reset {
        observed_at
            .checked_add_nanos(CONSERVATIVE_MONTH_NANOS)
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
    } else {
        current.month_resets_at()
    };
    let day = if observed_at >= current.day_resets_at() {
        observed_at
            .checked_add_nanos(DAY_NANOS)
            .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
    } else {
        current.day_resets_at()
    }
    .min(month);
    // Never push a broader pending reset forward merely to fit a fresh narrow window. Shortening
    // this duplicate local request window is safe because the generic shared sliding windows
    // remain authoritative for hour/day attempts.
    let hour = observed_at
        .checked_add_nanos(HOUR_NANOS)
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)?
        .min(day);
    quota
        .advance_windows(
            observed_at,
            TiingoQuotaWindows::try_new(observed_at, hour, day, month)
                .map_err(|_| TiingoProviderAuthorityError::Corrupt)?,
        )
        .map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
    Ok(Some(month_reset))
}

fn valid_settlement(permit: &TiingoProviderPermit, settlement: &TiingoResponseSettlement) -> bool {
    match settlement {
        TiingoResponseSettlement::NotDispatched => true,
        TiingoResponseSettlement::Complete { response_bytes, .. } => {
            *response_bytes <= permit.maximum_response_bytes().get()
        }
        TiingoResponseSettlement::Incomplete {
            observed_response_bytes,
            charged_response_bytes,
        } => {
            observed_response_bytes <= charged_response_bytes
                && *charged_response_bytes >= permit.maximum_response_bytes().get()
        }
    }
}

fn requirements_evidence(
    requirements: &TiingoProviderAuthorityRequirements,
    key: &ProviderRateExtensionKey,
) -> EvidenceDigest {
    digest_fields(&[
        b"market-squawk/tiingo/provider-rate-extension-installation/v1",
        key.provider_subject().as_str().as_bytes(),
        key.extension_id().as_str().as_bytes(),
        key.schema_id().as_str().as_bytes(),
        &key.policy_digest().bytes(),
        &key.declaration_digest().bytes(),
        requirements.source_id().as_str().as_bytes(),
        requirements
            .source_contract_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
        requirements.native_contract_revision().as_str().as_bytes(),
        requirements.entitlement_generation().as_str().as_bytes(),
        &requirements
            .provider_unique_symbols_per_month()
            .to_be_bytes(),
        &requirements
            .application_unique_symbols_per_month()
            .to_be_bytes(),
        &requirements.provider_bytes_per_month().to_be_bytes(),
        &requirements.application_bytes_per_month().to_be_bytes(),
    ])
}

fn encode_state(state: &DurableTiingoState) -> Result<Vec<u8>, TiingoProviderAuthorityError> {
    serde_json::to_vec(state).map_err(|_| TiingoProviderAuthorityError::Corrupt)
}

fn decode_state(bytes: &[u8]) -> Result<DurableTiingoState, TiingoProviderAuthorityError> {
    let state: DurableTiingoState =
        serde_json::from_slice(bytes).map_err(|_| TiingoProviderAuthorityError::Corrupt)?;
    validate_state(&state)?;
    if encode_state(&state)? != bytes {
        return Err(TiingoProviderAuthorityError::Corrupt);
    }
    Ok(state)
}

fn map_store_error(error: ProviderRateStoreError) -> TiingoProviderAuthorityError {
    match error {
        ProviderRateStoreError::Conflict => TiingoProviderAuthorityError::Conflict,
        ProviderRateStoreError::Corrupt => TiingoProviderAuthorityError::Corrupt,
        ProviderRateStoreError::AlreadyOwned
        | ProviderRateStoreError::Unavailable
        | ProviderRateStoreError::Capacity
        | ProviderRateStoreError::Clock => TiingoProviderAuthorityError::Unavailable,
    }
}

fn identifier(value: &str) -> Result<SourceIdentifier, TiingoProviderAuthorityError> {
    SourceIdentifier::try_from(value).map_err(|_| TiingoProviderAuthorityError::Corrupt)
}

fn sha256_evidence(bytes: [u8; 32]) -> Result<EvidenceDigest, TiingoProviderAuthorityError> {
    if bytes == [0; 32] {
        return Err(TiingoProviderAuthorityError::Corrupt);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn digest_fields(fields: &[&[u8]]) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(field);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

const fn endpoint_id(endpoint: TiingoEndpointFamily) -> u8 {
    match endpoint {
        TiingoEndpointFamily::Metadata => 1,
        TiingoEndpointFamily::LatestDailyPrices => 2,
        TiingoEndpointFamily::HistoricalDailyPrices => 3,
    }
}

fn endpoint_from_id(value: u8) -> Result<TiingoEndpointFamily, TiingoProviderAuthorityError> {
    match value {
        1 => Ok(TiingoEndpointFamily::Metadata),
        2 => Ok(TiingoEndpointFamily::LatestDailyPrices),
        3 => Ok(TiingoEndpointFamily::HistoricalDailyPrices),
        _ => Err(TiingoProviderAuthorityError::Corrupt),
    }
}

const fn schema_reason_id(reason: TiingoSchemaChangeReason) -> u8 {
    match reason {
        TiingoSchemaChangeReason::InvalidTopLevel => 1,
        TiingoSchemaChangeReason::MissingField => 2,
        TiingoSchemaChangeReason::UnknownField => 3,
        TiingoSchemaChangeReason::InvalidFieldType => 4,
        TiingoSchemaChangeReason::InvalidFieldValue => 5,
        TiingoSchemaChangeReason::InvalidRowSequence => 6,
        TiingoSchemaChangeReason::RowLimitExceeded => 7,
        TiingoSchemaChangeReason::SymbolMismatch => 8,
    }
}

fn schema_reason_from_id(
    value: u8,
) -> Result<TiingoSchemaChangeReason, TiingoProviderAuthorityError> {
    match value {
        1 => Ok(TiingoSchemaChangeReason::InvalidTopLevel),
        2 => Ok(TiingoSchemaChangeReason::MissingField),
        3 => Ok(TiingoSchemaChangeReason::UnknownField),
        4 => Ok(TiingoSchemaChangeReason::InvalidFieldType),
        5 => Ok(TiingoSchemaChangeReason::InvalidFieldValue),
        6 => Ok(TiingoSchemaChangeReason::InvalidRowSequence),
        7 => Ok(TiingoSchemaChangeReason::RowLimitExceeded),
        8 => Ok(TiingoSchemaChangeReason::SymbolMismatch),
        _ => Err(TiingoProviderAuthorityError::Corrupt),
    }
}
