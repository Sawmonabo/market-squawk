//! Redacted, provenance-bearing effective-configuration reporting.

use std::path::Path;

use serde::Serialize;

use super::{AppConfig, ConfigOrigin, ConfigSetting};

/// One effective value paired with the exact precedence layer that supplied it.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct EffectiveSettingView<T> {
    value: T,
    origin: ConfigOrigin,
}

impl<T> EffectiveSettingView<T> {
    const fn new(value: T, origin: ConfigOrigin) -> Self {
        Self { value, origin }
    }
}

/// Complete redacted view of the validated effective local configuration.
///
/// Secret material and secret locators are never represented. Source credentials are reported
/// only as a configured/not-configured state so CLI, doctor, and protocol consumers can share one
/// serialization boundary without gaining secret authority.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveConfigView<'config> {
    schema_version: &'static str,
    data_directory: EffectiveSettingView<&'config Path>,
    products: EffectiveSettingView<&'config [String]>,
    stale_after_ms: EffectiveSettingView<u128>,
    capture_queue_capacity: EffectiveSettingView<usize>,
    capture_memory_ceiling_bytes: EffectiveSettingView<usize>,
    capture_destination_registry_memory_ceiling_bytes: EffectiveSettingView<usize>,
    paper_bot_enabled: EffectiveSettingView<bool>,
    capture_flush_interval_ms: EffectiveSettingView<u128>,
    capture_shutdown_ms: EffectiveSettingView<u128>,
    source_shutdown_ms: EffectiveSettingView<u128>,
    source_secret_configured: EffectiveSettingView<bool>,
    coinbase_configured: EffectiveSettingView<bool>,
    kraken_configured: EffectiveSettingView<bool>,
}

impl AppConfig {
    /// Returns a borrowed, redacted view with value-level configuration provenance.
    #[must_use]
    pub fn redacted_view(&self) -> EffectiveConfigView<'_> {
        let provenance = self.provenance();
        EffectiveConfigView {
            schema_version: "market-squawk-effective-config-v1",
            data_directory: EffectiveSettingView::new(
                self.data_dir(),
                provenance.origin(ConfigSetting::DataDirectory),
            ),
            products: EffectiveSettingView::new(
                self.products(),
                provenance.origin(ConfigSetting::Products),
            ),
            stale_after_ms: EffectiveSettingView::new(
                self.stale_after().as_millis(),
                provenance.origin(ConfigSetting::StaleAfter),
            ),
            capture_queue_capacity: EffectiveSettingView::new(
                self.capture_queue_capacity().get(),
                provenance.origin(ConfigSetting::CaptureQueueCapacity),
            ),
            capture_memory_ceiling_bytes: EffectiveSettingView::new(
                self.capture_memory_ceiling_bytes().get(),
                provenance.origin(ConfigSetting::CaptureMemoryCeiling),
            ),
            capture_destination_registry_memory_ceiling_bytes: EffectiveSettingView::new(
                self.capture_destination_registry_memory_ceiling_bytes()
                    .get(),
                provenance.origin(ConfigSetting::CaptureDestinationRegistryMemoryCeiling),
            ),
            paper_bot_enabled: EffectiveSettingView::new(
                self.paper_bot_enabled(),
                provenance.origin(ConfigSetting::PaperBotEnabled),
            ),
            capture_flush_interval_ms: EffectiveSettingView::new(
                self.capture_flush_interval().as_millis(),
                provenance.origin(ConfigSetting::CaptureFlushInterval),
            ),
            capture_shutdown_ms: EffectiveSettingView::new(
                self.capture_shutdown().as_millis(),
                provenance.origin(ConfigSetting::CaptureShutdown),
            ),
            source_shutdown_ms: EffectiveSettingView::new(
                self.source_shutdown().as_millis(),
                provenance.origin(ConfigSetting::SourceShutdown),
            ),
            source_secret_configured: EffectiveSettingView::new(
                self.source_secret().is_some(),
                provenance.origin(ConfigSetting::SourceSecret),
            ),
            coinbase_configured: EffectiveSettingView::new(
                self.coinbase().is_some(),
                provenance.origin(ConfigSetting::Coinbase),
            ),
            kraken_configured: EffectiveSettingView::new(
                self.kraken().is_some(),
                provenance.origin(ConfigSetting::Kraken),
            ),
        }
    }
}
