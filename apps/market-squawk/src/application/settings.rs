//! Typed, origin-aware, durable product settings with preview and rollback.

use std::{collections::BTreeMap, fmt, path::Path, sync::Mutex};

use market_squawk_platform::{
    AppConfig, ConfigOrigin, ConfigSetting, LocalAuthorityStateStore, LocalAuthorityStateStoreError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::logs::LogSeverity;

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "settings-authority";
const MAXIMUM_HISTORY_REVISIONS: usize = 16;
const MAXIMUM_CHANGES: usize = 16;

/// Stable identity of every settings value exposed to clients.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingKey {
    LogRetentionDays,
    LogMinimumSeverity,
    UpdateChannel,
    AutomaticUpdateChecks,
    StorageSoftLimitBytes,
    DefaultQueryRowLimit,
    MaximumConcurrentJobs,
    MarketFreshnessMillis,
    BackupRetentionCount,
}

impl SettingKey {
    const ALL: [Self; 9] = [
        Self::LogRetentionDays,
        Self::LogMinimumSeverity,
        Self::UpdateChannel,
        Self::AutomaticUpdateChecks,
        Self::StorageSoftLimitBytes,
        Self::DefaultQueryRowLimit,
        Self::MaximumConcurrentJobs,
        Self::MarketFreshnessMillis,
        Self::BackupRetentionCount,
    ];

    /// Returns the complete closed setting-key set for composition-time consumer binding.
    #[must_use]
    pub(crate) const fn all() -> [Self; 9] {
        Self::ALL
    }

    /// Returns whether applying this setting requires a service lifecycle event.
    #[must_use]
    pub const fn restart_impact(self) -> RestartImpact {
        match self {
            Self::LogRetentionDays
            | Self::LogMinimumSeverity
            | Self::AutomaticUpdateChecks
            | Self::DefaultQueryRowLimit
            | Self::StorageSoftLimitBytes
            | Self::MaximumConcurrentJobs
            | Self::BackupRetentionCount => RestartImpact::ServiceReload,
            Self::UpdateChannel | Self::MarketFreshnessMillis => RestartImpact::ServiceRestart,
        }
    }
}

/// Product-owned update stream. It never contains an arbitrary URL.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateChannel {
    Stable,
    Preview,
}

/// Closed, key-bearing value set; invalid key/type pairs cannot be represented.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SettingValue {
    LogRetentionDays(u16),
    LogMinimumSeverity(LogSeverity),
    UpdateChannel(UpdateChannel),
    AutomaticUpdateChecks(bool),
    StorageSoftLimitBytes(u64),
    DefaultQueryRowLimit(u32),
    MaximumConcurrentJobs(u16),
    MarketFreshnessMillis(u64),
    BackupRetentionCount(u16),
}

impl SettingValue {
    /// Returns the only setting identity valid for this value variant.
    #[must_use]
    pub const fn key(&self) -> SettingKey {
        match self {
            Self::LogRetentionDays(_) => SettingKey::LogRetentionDays,
            Self::LogMinimumSeverity(_) => SettingKey::LogMinimumSeverity,
            Self::UpdateChannel(_) => SettingKey::UpdateChannel,
            Self::AutomaticUpdateChecks(_) => SettingKey::AutomaticUpdateChecks,
            Self::StorageSoftLimitBytes(_) => SettingKey::StorageSoftLimitBytes,
            Self::DefaultQueryRowLimit(_) => SettingKey::DefaultQueryRowLimit,
            Self::MaximumConcurrentJobs(_) => SettingKey::MaximumConcurrentJobs,
            Self::MarketFreshnessMillis(_) => SettingKey::MarketFreshnessMillis,
            Self::BackupRetentionCount(_) => SettingKey::BackupRetentionCount,
        }
    }

    fn validate(&self) -> Result<(), SettingsError> {
        let valid = match self {
            Self::LogRetentionDays(value) => (1..=365).contains(value),
            Self::LogMinimumSeverity(_)
            | Self::UpdateChannel(_)
            | Self::AutomaticUpdateChecks(_) => true,
            Self::StorageSoftLimitBytes(value) => {
                (1024_u64.pow(3)..=16 * 1024_u64.pow(4)).contains(value)
            }
            Self::DefaultQueryRowLimit(value) => (100..=1_000_000).contains(value),
            Self::MaximumConcurrentJobs(value) => (1..=64).contains(value),
            Self::MarketFreshnessMillis(value) => (250..=600_000).contains(value),
            Self::BackupRetentionCount(value) => (1..=64).contains(value),
        };
        if valid {
            Ok(())
        } else {
            Err(SettingsError::InvalidValue { key: self.key() })
        }
    }
}

/// Exact precedence layer that supplied the displayed effective value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingOrigin {
    SafeDefault,
    LocalPersisted,
    LocalConfiguration,
    Environment,
    CliOverride,
    ManagedPolicy,
}

impl SettingOrigin {
    const fn is_locally_mutable(self) -> bool {
        matches!(self, Self::SafeDefault | Self::LocalPersisted)
    }
}

/// Highest lifecycle impact among settings in one approved change.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartImpact {
    None,
    ServiceReload,
    ServiceRestart,
}

/// Effective value, origin, mutability, and lifecycle consequence returned to clients.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SettingEntry {
    key: SettingKey,
    value: SettingValue,
    origin: SettingOrigin,
    locally_mutable: bool,
    restart_impact: RestartImpact,
}

impl SettingEntry {
    /// Admits one effective value from the composition root without exposing secret material.
    pub fn try_new(value: SettingValue, origin: SettingOrigin) -> Result<Self, SettingsError> {
        value.validate()?;
        let key = value.key();
        Ok(Self {
            key,
            value,
            origin,
            locally_mutable: origin.is_locally_mutable(),
            restart_impact: key.restart_impact(),
        })
    }

    /// Returns the stable setting identity.
    #[must_use]
    pub const fn key(&self) -> SettingKey {
        self.key
    }

    /// Returns the validated typed value for service-owned consumer application.
    #[must_use]
    pub(crate) const fn value(&self) -> &SettingValue {
        &self.value
    }
}

/// Complete effective seed supplied after safe-default/file/environment/CLI composition.
#[derive(Clone, Debug)]
pub struct SettingsSeed {
    entries: BTreeMap<SettingKey, SettingEntry>,
}

impl SettingsSeed {
    /// Requires every public product setting exactly once.
    pub fn try_new(entries: Vec<SettingEntry>) -> Result<Self, SettingsError> {
        if entries.len() != SettingKey::ALL.len() {
            return Err(SettingsError::IncompleteSeed);
        }
        let mut by_key = BTreeMap::new();
        for entry in entries {
            let key = entry.key;
            if entry.value.key() != key || by_key.insert(key, entry).is_some() {
                return Err(SettingsError::IncompleteSeed);
            }
        }
        validate_complete_entries(&by_key)?;
        Ok(Self { entries: by_key })
    }

    /// Recommended complete local defaults used when no higher-precedence value is supplied.
    pub fn recommended_defaults() -> Result<Self, SettingsError> {
        Self::with_market_freshness(5_000, SettingOrigin::SafeDefault)
    }

    /// Derives the overlapping effective setting and its exact configuration-layer origin.
    pub(crate) fn from_config(config: &AppConfig) -> Result<Self, SettingsError> {
        let freshness = u64::try_from(config.stale_after().as_millis()).map_err(|_| {
            SettingsError::InvalidValue {
                key: SettingKey::MarketFreshnessMillis,
            }
        })?;
        let origin = match config.provenance().origin(ConfigSetting::StaleAfter) {
            ConfigOrigin::SafeDefault => SettingOrigin::SafeDefault,
            ConfigOrigin::LocalFile => SettingOrigin::LocalConfiguration,
            ConfigOrigin::Environment => SettingOrigin::Environment,
            ConfigOrigin::Cli => SettingOrigin::CliOverride,
        };
        Self::with_market_freshness(freshness, origin)
    }

    fn with_market_freshness(
        freshness_millis: u64,
        freshness_origin: SettingOrigin,
    ) -> Result<Self, SettingsError> {
        Self::try_new(vec![
            SettingEntry::try_new(
                SettingValue::LogRetentionDays(30),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::LogMinimumSeverity(LogSeverity::Info),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::UpdateChannel(UpdateChannel::Stable),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::AutomaticUpdateChecks(true),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::StorageSoftLimitBytes(50 * 1024_u64.pow(3)),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::DefaultQueryRowLimit(10_000),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::MaximumConcurrentJobs(4),
                SettingOrigin::SafeDefault,
            )?,
            SettingEntry::try_new(
                SettingValue::MarketFreshnessMillis(freshness_millis),
                freshness_origin,
            )?,
            SettingEntry::try_new(
                SettingValue::BackupRetentionCount(8),
                SettingOrigin::SafeDefault,
            )?,
        ])
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SettingsRevision {
    revision: u64,
    local_values: BTreeMap<SettingKey, SettingValue>,
    digest: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SettingsDocument {
    format_version: u16,
    revision: u64,
    local_values: BTreeMap<SettingKey, SettingValue>,
    history: Vec<SettingsRevision>,
    digest: [u8; 32],
}

impl SettingsDocument {
    fn initial(seed: SettingsSeed) -> Result<Self, SettingsError> {
        let revision = 1;
        let local_values = seed
            .entries
            .into_iter()
            .filter_map(|(key, entry)| {
                (entry.origin == SettingOrigin::LocalPersisted).then_some((key, entry.value))
            })
            .collect::<BTreeMap<_, _>>();
        let digest = local_settings_digest(revision, &local_values)?;
        Ok(Self {
            format_version: FORMAT_VERSION,
            revision,
            local_values,
            history: Vec::new(),
            digest,
        })
    }

    fn validate(self) -> Result<Self, SettingsError> {
        if self.format_version != FORMAT_VERSION
            || self.revision == 0
            || self.history.len() > MAXIMUM_HISTORY_REVISIONS
            || self.digest != local_settings_digest(self.revision, &self.local_values)?
        {
            return Err(SettingsError::CorruptState);
        }
        validate_local_values(&self.local_values)?;
        let mut prior_revision = 0;
        for historical in &self.history {
            validate_local_values(&historical.local_values)?;
            if historical.revision == 0
                || historical.revision >= self.revision
                || historical.revision <= prior_revision
                || historical.digest
                    != local_settings_digest(historical.revision, &historical.local_values)?
            {
                return Err(SettingsError::CorruptState);
            }
            prior_revision = historical.revision;
        }
        Ok(self)
    }
}

/// Versioned non-secret settings state retained as workspace-backup evidence.
///
/// The portable startup entries are evidence only. Restore uses the durable local document and
/// deliberately does not promote captured environment or command-line origins into authority.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct WorkspaceSettingsBackup {
    format_version: u16,
    document: SettingsDocument,
    startup: PortableStartupConfiguration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PortableStartupConfiguration {
    revision: u64,
    entries: Vec<SettingEntry>,
    effective_digest: [u8; 32],
}

impl WorkspaceSettingsBackup {
    fn try_new(
        document: SettingsDocument,
        entries: BTreeMap<SettingKey, SettingEntry>,
    ) -> Result<Self, SettingsError> {
        let effective_digest = effective_settings_digest(document.revision, &entries)?;
        Ok(Self {
            format_version: FORMAT_VERSION,
            startup: PortableStartupConfiguration {
                revision: document.revision,
                entries: entries.into_values().collect(),
                effective_digest,
            },
            document,
        })
    }

    pub(crate) fn validate(self) -> Result<Self, SettingsError> {
        if self.format_version != FORMAT_VERSION || self.startup.revision != self.document.revision
        {
            return Err(SettingsError::CorruptState);
        }
        self.document.clone().validate()?;
        let entries = self
            .startup
            .entries
            .iter()
            .cloned()
            .map(|entry| (entry.key, entry))
            .collect::<BTreeMap<_, _>>();
        if entries.len() != self.startup.entries.len()
            || validate_complete_entries(&entries).is_err()
            || self.startup.effective_digest
                != effective_settings_digest(self.startup.revision, &entries)?
        {
            return Err(SettingsError::CorruptState);
        }
        Ok(self)
    }

    /// Returns the source startup revision and effective-digest evidence for journal binding.
    #[must_use]
    pub(crate) const fn startup_binding(&self) -> (u64, [u8; 32]) {
        (self.startup.revision, self.startup.effective_digest)
    }
}

/// Immutable settings snapshot returned to clients.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsSnapshot {
    revision: u64,
    entries: Vec<SettingEntry>,
    digest: [u8; 32],
}

impl SettingsSnapshot {
    /// Returns the monotonic durable revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns all effective entries in stable setting-key order.
    #[must_use]
    pub fn entries(&self) -> &[SettingEntry] {
        &self.entries
    }

    /// Returns the digest of this complete effective typed snapshot.
    #[must_use]
    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Immutable, digest-bound preview of a typed local settings change.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsChangePreview {
    current_revision: u64,
    changes: Vec<SettingValue>,
    restart_impact: RestartImpact,
    preview_sha256: [u8; 32],
}

impl SettingsChangePreview {
    /// Converts the displayed exact preview into process-local application authority.
    #[must_use]
    pub fn approve(self) -> SettingsChangeApproval {
        SettingsChangeApproval {
            current_revision: self.current_revision,
            changes: self.changes,
            restart_impact: self.restart_impact,
            preview_sha256: self.preview_sha256,
        }
    }
}

/// Non-serializable approval bound to one exact displayed preview.
#[derive(Debug)]
pub struct SettingsChangeApproval {
    current_revision: u64,
    changes: Vec<SettingValue>,
    restart_impact: RestartImpact,
    preview_sha256: [u8; 32],
}

/// Bounded retained-history evidence used by the installed settings lifecycle authority.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SettingsRollbackTarget {
    restart_impact: RestartImpact,
    resulting_digest: [u8; 32],
    preview_sha256: [u8; 32],
}

impl SettingsRollbackTarget {
    /// Returns the lifecycle impact of restoring the retained target values.
    #[must_use]
    pub(crate) const fn restart_impact(self) -> RestartImpact {
        self.restart_impact
    }

    /// Returns the digest binding the current revision, retained target, impact, and result.
    #[must_use]
    pub(crate) const fn preview_sha256(self) -> [u8; 32] {
        self.preview_sha256
    }

    /// Returns the effective digest expected after the rollback advances the revision.
    #[must_use]
    pub(crate) const fn resulting_digest(self) -> [u8; 32] {
        self.resulting_digest
    }
}

/// Durable save/rollback evidence. Rollback always advances rather than resurrecting a revision.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SettingsReceipt {
    previous_revision: u64,
    active_revision: u64,
    active_digest: [u8; 32],
    restart_impact: RestartImpact,
    rolled_back_from_revision: Option<u64>,
}

impl SettingsReceipt {
    /// Returns the active monotonic revision after persistence completed.
    #[must_use]
    pub const fn active_revision(self) -> u64 {
        self.active_revision
    }

    /// Returns the highest lifecycle impact of the effective change.
    #[must_use]
    pub const fn restart_impact(self) -> RestartImpact {
        self.restart_impact
    }

    /// Returns the digest of the complete effective settings after persistence.
    #[must_use]
    pub(crate) const fn active_digest(self) -> [u8; 32] {
        self.active_digest
    }
}

/// Exclusive durable settings owner. It persists only validated closed values, never raw TOML.
pub struct DurableSettingsStore {
    store: LocalAuthorityStateStore,
    seed: SettingsSeed,
    document: Mutex<SettingsDocument>,
}

impl DurableSettingsStore {
    /// Opens or initializes the settings authority beneath the prepared control root.
    pub fn try_open(control_root: &Path, seed: SettingsSeed) -> Result<Self, SettingsError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        let document = match store.load()? {
            Some(bytes) => serde_json::from_slice::<SettingsDocument>(&bytes)
                .map_err(|_| SettingsError::CorruptState)?
                .validate()?,
            None => {
                let document = SettingsDocument::initial(seed.clone())?;
                store.store(&encode(&document)?)?;
                document
            }
        };
        Ok(Self {
            store,
            seed,
            document: Mutex::new(document),
        })
    }

    /// Returns all typed values with effective origins and restart impact in stable key order.
    pub fn snapshot(&self) -> Result<SettingsSnapshot, SettingsError> {
        let document = self
            .document
            .lock()
            .map_err(|_| SettingsError::Unavailable)?;
        let entries = effective_entries(&self.seed, &document.local_values)?;
        Ok(SettingsSnapshot {
            revision: document.revision,
            digest: effective_settings_digest(document.revision, &entries)?,
            entries: entries.into_values().collect(),
        })
    }

    /// Captures validated durable local state and non-secret startup evidence.
    ///
    /// This operation does not export a configuration path, data directory, source locator,
    /// credential, or ambient-input authority. Callers that require an atomic workspace snapshot
    /// retain their wider lifecycle transaction while calling it.
    pub(crate) fn export_workspace_backup(&self) -> Result<WorkspaceSettingsBackup, SettingsError> {
        let document = self
            .document
            .lock()
            .map_err(|_| SettingsError::Unavailable)?
            .clone()
            .validate()?;
        WorkspaceSettingsBackup::try_new(
            document.clone(),
            effective_entries(&self.seed, &document.local_values)?,
        )
    }

    /// Rehydrates a validated backup only into an absent durable settings authority, then reopens
    /// it through the normal checked constructor.
    pub(crate) fn restore_workspace_backup_absent(
        control_root: &Path,
        seed: SettingsSeed,
        backup: WorkspaceSettingsBackup,
    ) -> Result<Self, SettingsError> {
        let backup = backup.validate()?;
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        if store.load()?.is_some() {
            return Err(SettingsError::RestoreTargetExists);
        }
        store.store(&encode(&backup.document)?)?;
        let reopened = Self::try_open(control_root, seed)?;
        if reopened.snapshot()?.revision() != backup.document.revision {
            return Err(SettingsError::CorruptState);
        }
        Ok(reopened)
    }

    /// Refuses restore unless the target settings authority contains no durable document.
    pub(crate) fn ensure_workspace_backup_target_absent(
        control_root: &Path,
    ) -> Result<(), SettingsError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))?;
        if store.load()?.is_some() {
            Err(SettingsError::RestoreTargetExists)
        } else {
            Ok(())
        }
    }

    /// Validates a bounded patch and reports its combined lifecycle impact before approval.
    pub fn preview(
        &self,
        expected_revision: u64,
        changes: Vec<SettingValue>,
    ) -> Result<SettingsChangePreview, SettingsError> {
        if changes.is_empty() || changes.len() > MAXIMUM_CHANGES {
            return Err(SettingsError::InvalidChangeSet);
        }
        let document = self
            .document
            .lock()
            .map_err(|_| SettingsError::Unavailable)?;
        if expected_revision != document.revision {
            return Err(SettingsError::StaleRevision);
        }
        let mut keys = BTreeMap::new();
        let mut restart_impact = RestartImpact::None;
        let entries = effective_entries(&self.seed, &document.local_values)?;
        for change in &changes {
            change.validate()?;
            let key = change.key();
            let entry = entries.get(&key).ok_or(SettingsError::CorruptState)?;
            if !entry.locally_mutable || keys.insert(key, ()).is_some() {
                return Err(SettingsError::ImmutableOrDuplicateSetting { key });
            }
            restart_impact = restart_impact.max(key.restart_impact());
        }
        let preview_sha256 = preview_digest(document.revision, &changes, restart_impact)?;
        Ok(SettingsChangePreview {
            current_revision: document.revision,
            changes,
            restart_impact,
            preview_sha256,
        })
    }

    /// Persists the exact approved patch before publishing its new active revision.
    pub fn apply(
        &self,
        approval: SettingsChangeApproval,
    ) -> Result<SettingsReceipt, SettingsError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| SettingsError::Unavailable)?;
        if document.revision != approval.current_revision
            || approval.preview_sha256
                != preview_digest(
                    approval.current_revision,
                    &approval.changes,
                    approval.restart_impact,
                )?
        {
            return Err(SettingsError::StaleOrInvalidApproval);
        }
        let previous_revision = document.revision;
        let mut candidate = document.clone();
        push_history(&mut candidate);
        candidate.revision = candidate
            .revision
            .checked_add(1)
            .ok_or(SettingsError::RevisionExhausted)?;
        for value in approval.changes {
            candidate.local_values.insert(value.key(), value);
        }
        candidate.digest = local_settings_digest(candidate.revision, &candidate.local_values)?;
        self.store.store(&encode(&candidate)?)?;
        let active_entries = effective_entries(&self.seed, &candidate.local_values)?;
        let receipt = SettingsReceipt {
            previous_revision,
            active_revision: candidate.revision,
            active_digest: effective_settings_digest(candidate.revision, &active_entries)?,
            restart_impact: approval.restart_impact,
            rolled_back_from_revision: None,
        };
        *document = candidate;
        Ok(receipt)
    }

    /// Verifies one exact retained rollback target without exposing historical values.
    pub(crate) fn preview_rollback(
        &self,
        expected_revision: u64,
        target_revision: u64,
    ) -> Result<SettingsRollbackTarget, SettingsError> {
        let document = self
            .document
            .lock()
            .map_err(|_| SettingsError::Unavailable)?;
        rollback_target(&self.seed, &document, expected_revision, target_revision)
    }

    /// Restores one retained snapshot as a new monotonic revision after optimistic fencing.
    pub fn rollback(
        &self,
        expected_revision: u64,
        target_revision: u64,
    ) -> Result<SettingsReceipt, SettingsError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| SettingsError::Unavailable)?;
        let preview = rollback_target(&self.seed, &document, expected_revision, target_revision)?;
        let target = document
            .history
            .iter()
            .find(|historical| historical.revision == target_revision)
            .cloned()
            .ok_or(SettingsError::UnknownRollbackRevision)?;
        let previous_revision = document.revision;
        let mut candidate = document.clone();
        push_history(&mut candidate);
        candidate.revision = candidate
            .revision
            .checked_add(1)
            .ok_or(SettingsError::RevisionExhausted)?;
        candidate.local_values = target.local_values;
        candidate.digest = local_settings_digest(candidate.revision, &candidate.local_values)?;
        self.store.store(&encode(&candidate)?)?;
        let receipt = SettingsReceipt {
            previous_revision,
            active_revision: candidate.revision,
            active_digest: preview.resulting_digest,
            restart_impact: preview.restart_impact,
            rolled_back_from_revision: Some(target_revision),
        };
        *document = candidate;
        Ok(receipt)
    }
}

impl fmt::Debug for DurableSettingsStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableSettingsStore([EXCLUSIVE LOCAL AUTHORITY])")
    }
}

fn validate_complete_entries(
    entries: &BTreeMap<SettingKey, SettingEntry>,
) -> Result<(), SettingsError> {
    if entries.len() != SettingKey::ALL.len()
        || SettingKey::ALL.into_iter().any(|key| {
            entries.get(&key).is_none_or(|entry| {
                entry.key != key
                    || entry.value.key() != key
                    || entry.locally_mutable != entry.origin.is_locally_mutable()
                    || entry.restart_impact != key.restart_impact()
                    || entry.value.validate().is_err()
            })
        })
    {
        return Err(SettingsError::IncompleteSeed);
    }
    Ok(())
}

fn validate_local_values(values: &BTreeMap<SettingKey, SettingValue>) -> Result<(), SettingsError> {
    if values.len() > SettingKey::ALL.len()
        || values
            .iter()
            .any(|(key, value)| value.key() != *key || value.validate().is_err())
    {
        return Err(SettingsError::CorruptState);
    }
    Ok(())
}

fn effective_entries(
    seed: &SettingsSeed,
    local_values: &BTreeMap<SettingKey, SettingValue>,
) -> Result<BTreeMap<SettingKey, SettingEntry>, SettingsError> {
    validate_complete_entries(&seed.entries)?;
    validate_local_values(local_values)?;
    seed.entries
        .iter()
        .map(|(key, seeded)| {
            if seeded.origin.is_locally_mutable()
                && let Some(local) = local_values.get(key)
            {
                return SettingEntry::try_new(local.clone(), SettingOrigin::LocalPersisted)
                    .map(|entry| (*key, entry));
            }
            Ok((*key, seeded.clone()))
        })
        .collect()
}

fn push_history(document: &mut SettingsDocument) {
    if document.history.len() == MAXIMUM_HISTORY_REVISIONS {
        document.history.remove(0);
    }
    document.history.push(SettingsRevision {
        revision: document.revision,
        local_values: document.local_values.clone(),
        digest: document.digest,
    });
}

fn changed_restart_impact(
    current: &BTreeMap<SettingKey, SettingEntry>,
    target: &BTreeMap<SettingKey, SettingEntry>,
) -> RestartImpact {
    SettingKey::ALL
        .into_iter()
        .filter(|key| current.get(key) != target.get(key))
        .map(SettingKey::restart_impact)
        .max()
        .unwrap_or(RestartImpact::None)
}

fn rollback_target(
    seed: &SettingsSeed,
    document: &SettingsDocument,
    expected_revision: u64,
    target_revision: u64,
) -> Result<SettingsRollbackTarget, SettingsError> {
    if document.revision != expected_revision {
        return Err(SettingsError::StaleRevision);
    }
    let target = document
        .history
        .iter()
        .find(|historical| historical.revision == target_revision)
        .ok_or(SettingsError::UnknownRollbackRevision)?;
    let active_revision = document
        .revision
        .checked_add(1)
        .ok_or(SettingsError::RevisionExhausted)?;
    let current_entries = effective_entries(seed, &document.local_values)?;
    let target_entries = effective_entries(seed, &target.local_values)?;
    let restart_impact = changed_restart_impact(&current_entries, &target_entries);
    let resulting_digest = effective_settings_digest(active_revision, &target_entries)?;
    let preview_sha256 = serde_json::to_vec(&(
        "market-squawk-settings-rollback-preview-v1",
        document.revision,
        target_revision,
        restart_impact,
        resulting_digest,
    ))
    .map(|bytes| Sha256::digest(bytes).into())
    .map_err(|_| SettingsError::Encoding)?;
    Ok(SettingsRollbackTarget {
        restart_impact,
        resulting_digest,
        preview_sha256,
    })
}

fn local_settings_digest(
    revision: u64,
    local_values: &BTreeMap<SettingKey, SettingValue>,
) -> Result<[u8; 32], SettingsError> {
    serde_json::to_vec(&("market-squawk-local-settings-v1", revision, local_values))
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(|_| SettingsError::Encoding)
}

fn effective_settings_digest(
    revision: u64,
    entries: &BTreeMap<SettingKey, SettingEntry>,
) -> Result<[u8; 32], SettingsError> {
    serde_json::to_vec(&("market-squawk-effective-settings-v1", revision, entries))
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(|_| SettingsError::Encoding)
}

fn preview_digest(
    revision: u64,
    changes: &[SettingValue],
    restart_impact: RestartImpact,
) -> Result<[u8; 32], SettingsError> {
    serde_json::to_vec(&(
        "market-squawk-settings-preview-v1",
        revision,
        changes,
        restart_impact,
    ))
    .map(|bytes| Sha256::digest(bytes).into())
    .map_err(|_| SettingsError::Encoding)
}

fn encode(document: &SettingsDocument) -> Result<Vec<u8>, SettingsError> {
    let encoded = serde_json::to_vec(document).map_err(|_| SettingsError::Encoding)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(SettingsError::CapacityExceeded);
    }
    Ok(encoded)
}

/// Typed settings failure without raw values, paths, or secret material.
#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("settings seed is incomplete or internally inconsistent")]
    IncompleteSeed,
    #[error("setting value is outside its validated range: {key:?}")]
    InvalidValue { key: SettingKey },
    #[error("settings change set is empty or exceeds its bound")]
    InvalidChangeSet,
    #[error("setting is externally controlled or duplicated: {key:?}")]
    ImmutableOrDuplicateSetting { key: SettingKey },
    #[error("settings revision is stale")]
    StaleRevision,
    #[error("settings approval is stale or invalid")]
    StaleOrInvalidApproval,
    #[error("settings rollback revision is not retained")]
    UnknownRollbackRevision,
    #[error("settings revision is exhausted")]
    RevisionExhausted,
    #[error("settings authority state is corrupt")]
    CorruptState,
    #[error("settings restore target already contains durable state")]
    RestoreTargetExists,
    #[error("settings authority is unavailable")]
    Unavailable,
    #[error("settings authority capacity is exhausted")]
    CapacityExceeded,
    #[error("settings encoding failed")]
    Encoding,
    #[error("settings persistence failed")]
    Persistence(#[from] LocalAuthorityStateStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_write_fails_and_rollback_advances_revision() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let store = DurableSettingsStore::try_open(
            directory.path(),
            SettingsSeed::recommended_defaults()?,
        )?;
        let first = store.snapshot()?;
        let approval = store
            .preview(first.revision(), vec![SettingValue::LogRetentionDays(14)])?
            .approve();
        let receipt = store.apply(approval)?;
        assert!(matches!(
            store.preview(
                first.revision(),
                vec![SettingValue::BackupRetentionCount(4)]
            ),
            Err(SettingsError::StaleRevision)
        ));
        let rollback = store.rollback(receipt.active_revision, first.revision())?;
        assert_eq!(rollback.active_revision, receipt.active_revision + 1);
        assert_eq!(rollback.rolled_back_from_revision, Some(first.revision()));
        Ok(())
    }

    #[test]
    fn workspace_backup_restores_only_once_and_reopens_the_exact_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_directory = tempfile::tempdir()?;
        let source = DurableSettingsStore::try_open(
            source_directory.path(),
            SettingsSeed::recommended_defaults()?,
        )?;
        let before = source.snapshot()?;
        source.apply(
            source
                .preview(before.revision(), vec![SettingValue::LogRetentionDays(14)])?
                .approve(),
        )?;
        let expected = source.snapshot()?;
        let backup = source.export_workspace_backup()?;

        let target_directory = tempfile::tempdir()?;
        let restored = DurableSettingsStore::restore_workspace_backup_absent(
            target_directory.path(),
            SettingsSeed::recommended_defaults()?,
            backup.clone(),
        )?;

        assert_eq!(restored.snapshot()?.revision(), expected.revision());
        assert_eq!(restored.snapshot()?.digest(), expected.digest());
        assert!(matches!(
            DurableSettingsStore::restore_workspace_backup_absent(
                target_directory.path(),
                SettingsSeed::recommended_defaults()?,
                backup,
            ),
            Err(SettingsError::RestoreTargetExists)
        ));
        Ok(())
    }
}
