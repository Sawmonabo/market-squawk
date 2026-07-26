//! Account-scoped pre-network admission for authenticated Coinbase Direct market data.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_platform::{AppConfig, LocalAuthorityStateStore, LocalPaths};
use market_squawk_sources::{AuthorizationMode, DataUseOperation, ProviderRateAuthority};

use crate::{ProviderActivationLease, ProviderOnboardingService};

use super::specs::{
    COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS, CoinbaseDirectActivationSpecError,
    CoinbaseDirectAdapterActivation, CoinbaseDirectProductActivation,
    ProviderAdapterActivationError,
};

const COINBASE_DIRECT_SURFACE: &str = "coinbase.exchange-direct-market-data";
const COINBASE_DIRECT_PROVIDER: &str = "coinbase-exchange";
const COINBASE_DIRECT_ACCOUNT_ROOT: &str = "coinbase-direct-account-authority";
const COINBASE_DIRECT_ACCOUNT_SUBJECT_PREFIX: &str = "coinbase-direct-account-";
const QUEUE_RECORD_OVERHEAD_BYTES: u64 = 512;
const ACCOUNT_SUPERVISOR_FIXED_BYTES: u64 = 256 * 1024;

/// Checked account-level memory and queue admission retained for runtime construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectRuntimeAdmission {
    required_bytes: NonZeroU64,
    maximum_bytes: NonZeroU64,
    capture_queue_records_per_product: NonZeroUsize,
    capture_queue_bytes_per_product: NonZeroUsize,
    supervisor_queue_records: NonZeroUsize,
    supervisor_queue_bytes: NonZeroUsize,
}

impl CoinbaseDirectRuntimeAdmission {
    /// Returns the conservative checked peak retained by Direct-specific runtime components.
    pub const fn required_bytes(self) -> NonZeroU64 {
        self.required_bytes
    }

    /// Returns the operator-selected Direct-specific runtime ceiling.
    pub const fn maximum_bytes(self) -> NonZeroU64 {
        self.maximum_bytes
    }

    /// Returns the preallocated raw-capture record count for each product task.
    pub const fn capture_queue_records_per_product(self) -> NonZeroUsize {
        self.capture_queue_records_per_product
    }

    /// Returns the preallocated raw-capture byte count for each product task.
    pub const fn capture_queue_bytes_per_product(self) -> NonZeroUsize {
        self.capture_queue_bytes_per_product
    }

    /// Returns the count bound for the account supervisor's owned-message ingress.
    pub const fn supervisor_queue_records(self) -> NonZeroUsize {
        self.supervisor_queue_records
    }

    /// Returns the byte bound for the account supervisor's owned-message ingress.
    pub const fn supervisor_queue_bytes(self) -> NonZeroUsize {
        self.supervisor_queue_bytes
    }
}

/// Exclusively owned, pre-network Coinbase Direct account activation.
///
/// Construction proves the exact active onboarding generation, binds its redacted verification
/// evidence to the stable account subject, acquires one lifetime cross-process lock beneath the
/// configured control root, and atomically moves the complete unique product set into ten fixed
/// slots. The lock coordinates Market Squawk runtimes that share this configured data root; it
/// does not claim authority over unrelated Coinbase clients or deliberately separate data roots.
/// It does not read credentials, open a socket, or claim runtime data quality.
pub struct CoinbaseDirectAccountActivation {
    lease: ProviderActivationLease,
    onboarding: Arc<ProviderOnboardingService>,
    app_config: AppConfig,
    _provider_rate: ProviderRateAuthority,
    account_subject: SourceIdentifier,
    admission: CoinbaseDirectRuntimeAdmission,
    product_count: usize,
    products: [Option<CoinbaseDirectProductActivation>; COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS],
    _account_authority: LocalAuthorityStateStore,
}

impl CoinbaseDirectAccountActivation {
    /// Returns the exact immutable onboarding authority used for this account runtime.
    pub const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    /// Returns the redacted, digest-derived provider-rate collision subject.
    pub const fn account_subject(&self) -> &SourceIdentifier {
        &self.account_subject
    }

    /// Returns the checked Direct-specific memory and queue admission.
    pub const fn runtime_admission(&self) -> CoinbaseDirectRuntimeAdmission {
        self.admission
    }

    /// Returns the number of occupied product/full slots.
    pub const fn product_count(&self) -> usize {
        self.product_count
    }

    /// Returns one exact occupied product slot.
    pub fn product(&self, index: usize) -> Option<&CoinbaseDirectProductActivation> {
        self.products.get(index).and_then(Option::as_ref)
    }

    /// Revalidates the exact onboarding generation outside the live event-to-action path.
    ///
    /// Runtime supervision uses this at a bounded lifecycle cadence; it never performs catalog
    /// access per market event.
    ///
    /// # Errors
    ///
    /// Fails when the session was rotated, revoked, cancelled, expired, or otherwise changed.
    pub async fn require_current(&self) -> Result<(), crate::ProviderOnboardingError> {
        self.onboarding
            .acquire_runtime_mutation_authority()
            .await
            .require_active(&self.lease)
    }

    /// Returns the configured local data root used by the account runtime.
    pub fn data_dir(&self) -> &Path {
        self.app_config.data_dir()
    }

    pub(crate) const fn app_config(&self) -> &AppConfig {
        &self.app_config
    }

    pub(crate) const fn onboarding(&self) -> &Arc<ProviderOnboardingService> {
        &self.onboarding
    }

    pub(crate) const fn provider_rate(&self) -> &ProviderRateAuthority {
        &self._provider_rate
    }

    pub(crate) fn take_products(
        &mut self,
    ) -> [Option<CoinbaseDirectProductActivation>; COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS] {
        self.product_count = 0;
        std::array::from_fn(|index| self.products[index].take())
    }
}

impl fmt::Debug for CoinbaseDirectAccountActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoinbaseDirectAccountActivation")
            .field("surface_id", self.lease.surface_id())
            .field("session_id", &self.lease.session_id())
            .field("account_subject", &"[REDACTED DIGEST SUBJECT]")
            .field("product_count", &self.product_count)
            .field("admission", &self.admission)
            .field("account_authority", &"[EXCLUSIVE LOCAL CAPABILITY]")
            .finish()
    }
}

pub(super) fn activate_coinbase_direct(
    onboarding: Arc<ProviderOnboardingService>,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    lease: ProviderActivationLease,
    spec: CoinbaseDirectAdapterActivation,
) -> Result<CoinbaseDirectAccountActivation, ProviderAdapterActivationError> {
    validate_direct_lease(&lease)?;
    let admission = checked_runtime_admission(&spec)?;
    let account_digest = lease
        .account_digest()
        .ok_or(ProviderAdapterActivationError::SourceBinding)?;
    let verification_evidence = lease
        .verification_evidence_digest()
        .ok_or(ProviderAdapterActivationError::SourceBinding)?;
    let account_subject = account_subject(account_digest)?;

    let onboarding_authority = onboarding.try_acquire_runtime_mutation_authority()?;
    onboarding_authority.require_active(&lease)?;
    provider_rate.bind_authorization_subject(
        AuthorizationMode::UserAuthorized,
        verification_evidence,
        &account_subject,
    )?;

    let paths = LocalPaths::prepare(app_config.data_dir())?;
    let authority_root = paths
        .control_root()?
        .root()
        .join(COINBASE_DIRECT_ACCOUNT_ROOT)
        .join(account_subject.as_str());
    let account_authority = LocalAuthorityStateStore::try_open(authority_root)?;
    onboarding_authority.require_active(&lease)?;

    let product_count = spec.products.len();
    let mut products = std::array::from_fn(|_| None);
    for (index, product) in spec.products.into_iter().enumerate() {
        let slot = products
            .get_mut(index)
            .ok_or(CoinbaseDirectActivationSpecError::SubscriptionCardinality)?;
        *slot = Some(product);
    }
    drop(onboarding_authority);

    Ok(CoinbaseDirectAccountActivation {
        lease,
        onboarding,
        app_config,
        _provider_rate: provider_rate,
        account_subject,
        admission,
        product_count,
        products,
        _account_authority: account_authority,
    })
}

fn validate_direct_lease(
    lease: &ProviderActivationLease,
) -> Result<(), ProviderAdapterActivationError> {
    let budget = lease
        .provider_budget_policy()
        .ok_or(ProviderAdapterActivationError::SourceBinding)?;
    if lease.surface_id().as_str() != COINBASE_DIRECT_SURFACE {
        return Err(ProviderAdapterActivationError::SurfaceMismatch);
    }
    if lease.generation().is_none()
        || lease.secret_reference().is_none()
        || lease.account_digest().is_none()
        || lease.verification_evidence_digest().is_none()
        || lease.verification_expires_at().is_none()
        || !lease.admits(DataUseOperation::Retrieve)
        || budget.scope().as_source_identifier().as_str() != COINBASE_DIRECT_PROVIDER
        || budget.scope().authorization_account().is_none()
    {
        return Err(ProviderAdapterActivationError::SourceBinding);
    }
    Ok(())
}

fn checked_runtime_admission(
    spec: &CoinbaseDirectAdapterActivation,
) -> Result<CoinbaseDirectRuntimeAdmission, ProviderAdapterActivationError> {
    let product_count = u64::try_from(spec.products.len())
        .map_err(|_| CoinbaseDirectActivationSpecError::MemoryAdmission)?;
    let session_bytes = spec.products.iter().try_fold(0_u64, |total, product| {
        total
            .checked_add(product.limits().checked_maximum_retained_bytes()?)
            .ok_or(CoinbaseDirectActivationSpecError::MemoryAdmission)
    })?;
    let capture_payload_bytes = checked_product(
        product_count,
        u64::try_from(spec.capture_queue_bytes_per_product.get())
            .map_err(|_| CoinbaseDirectActivationSpecError::MemoryAdmission)?,
    )?;
    let capture_record_bytes = checked_product(
        product_count,
        checked_product(
            u64::try_from(spec.capture_queue_records_per_product.get())
                .map_err(|_| CoinbaseDirectActivationSpecError::MemoryAdmission)?,
            QUEUE_RECORD_OVERHEAD_BYTES,
        )?,
    )?;
    let supervisor_payload_bytes = u64::try_from(spec.supervisor_queue_bytes.get())
        .map_err(|_| CoinbaseDirectActivationSpecError::MemoryAdmission)?;
    let supervisor_record_bytes = checked_product(
        u64::try_from(spec.supervisor_queue_records.get())
            .map_err(|_| CoinbaseDirectActivationSpecError::MemoryAdmission)?,
        QUEUE_RECORD_OVERHEAD_BYTES,
    )?;
    let transient_capture_payload_bytes = capture_payload_bytes;
    let transient_capture_record_bytes = capture_record_bytes;
    let required = [
        ACCOUNT_SUPERVISOR_FIXED_BYTES,
        session_bytes,
        capture_payload_bytes,
        capture_record_bytes,
        transient_capture_payload_bytes,
        transient_capture_record_bytes,
        supervisor_payload_bytes,
        supervisor_record_bytes,
    ]
    .into_iter()
    .try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or(CoinbaseDirectActivationSpecError::MemoryAdmission)
    })?;
    let required =
        NonZeroU64::new(required).ok_or(CoinbaseDirectActivationSpecError::MemoryAdmission)?;
    if required > spec.maximum_runtime_bytes {
        return Err(CoinbaseDirectActivationSpecError::MemoryAdmission.into());
    }
    Ok(CoinbaseDirectRuntimeAdmission {
        required_bytes: required,
        maximum_bytes: spec.maximum_runtime_bytes,
        capture_queue_records_per_product: spec.capture_queue_records_per_product,
        capture_queue_bytes_per_product: spec.capture_queue_bytes_per_product,
        supervisor_queue_records: spec.supervisor_queue_records,
        supervisor_queue_bytes: spec.supervisor_queue_bytes,
    })
}

fn checked_product(left: u64, right: u64) -> Result<u64, CoinbaseDirectActivationSpecError> {
    left.checked_mul(right)
        .ok_or(CoinbaseDirectActivationSpecError::MemoryAdmission)
}

fn account_subject(
    account_digest: EvidenceDigest,
) -> Result<SourceIdentifier, ProviderAdapterActivationError> {
    if account_digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(ProviderAdapterActivationError::SourceBinding);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = account_digest.bytes();
    let mut subject = String::with_capacity(
        COINBASE_DIRECT_ACCOUNT_SUBJECT_PREFIX
            .len()
            .saturating_add(bytes.len().saturating_mul(2)),
    );
    subject.push_str(COINBASE_DIRECT_ACCOUNT_SUBJECT_PREFIX);
    for byte in bytes {
        subject.push(char::from(HEX[usize::from(byte >> 4)]));
        subject.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(subject).map_err(|_| ProviderAdapterActivationError::SourceBinding)
}
