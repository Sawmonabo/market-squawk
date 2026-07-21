//! Deterministic local configuration composition and secret boundaries.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    io::Read,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;
use thiserror::Error;
use zeroize::Zeroize;

mod instruments;

pub use instruments::{
    COINBASE_EXCHANGE_ENDPOINT, CoinbaseAuthorizationAttestation, CoinbaseConfigurationError,
    CoinbaseControlLimits, CoinbaseInstrumentMapping, CoinbaseSourceConfig,
    KRAKEN_WEBSOCKET_V2_ENDPOINT, KrakenAuthorizationAttestation, KrakenConfigurationError,
    KrakenInstrumentMapping, KrakenSourceConfig,
};

const ENV_PREFIX: &str = "MARKET_SQUAWK_";
const DEFAULT_STALE_AFTER_MS: u64 = 5_000;
const DEFAULT_QUEUE_CAPACITY: usize = 16_384;
const DEFAULT_CAPTURE_MEMORY_CEILING_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES: usize = 1024 * 1024;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_SHUTDOWN_MS: u64 = 5_000;
const DEFAULT_SOURCE_SHUTDOWN_MS: u64 = 5_000;
const MAX_QUEUE_CAPACITY: usize = 1_048_576;
const MAX_CAPTURE_MEMORY_CEILING_BYTES: usize = 4_294_967_295;
const MAX_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES: usize = 64 * 1024 * 1024;
const MIN_STALE_AFTER_MS: u64 = 250;
const MAX_STALE_AFTER_MS: u64 = 600_000;
const MAX_SHUTDOWN_MS: u64 = 60_000;
const MAX_SOURCE_SHUTDOWN_MS: u64 = 60_000;
const MAX_SECRET_REFERENCE_BYTES: usize = 512;
const MAX_SECRET_VALUE_BYTES: usize = 64 * 1024;
const MAX_CONFIG_FILE_BYTES: u64 = 1024 * 1024;

/// A redacted locator resolved by an explicitly configured local secret provider.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretReference(String);

impl SecretReference {
    /// Exposes the locator only to a secret provider implementation.
    ///
    /// The returned value can contain account or key labels and must not enter logs or errors.
    pub fn expose_reference(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for SecretReference {
    type Error = SecretError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > MAX_SECRET_REFERENCE_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(SecretError::InvalidReference);
        }
        if !(value.starts_with("keyring:") || value.starts_with("encrypted-file:")) {
            return Err(SecretError::UnsupportedProvider);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Debug for SecretReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretReference([REDACTED])")
    }
}

/// Secret material whose debug representation and drop behavior are hardened.
pub struct SecretValue(String);

impl SecretValue {
    /// Constructs bounded secret material.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::InvalidValue`] for empty or oversized values.
    pub fn new(mut value: String) -> Result<Self, SecretError> {
        if value.is_empty() || value.len() > MAX_SECRET_VALUE_BYTES {
            value.zeroize();
            return Err(SecretError::InvalidValue);
        }
        Ok(Self(value))
    }

    /// Borrows secret material for its immediate authorized use.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Replaceable local secret-provider boundary.
pub trait SecretProvider: fmt::Debug + Send + Sync {
    /// Resolves a reference without retaining secret material in configuration state.
    fn resolve(&self, reference: &SecretReference) -> Result<SecretValue, SecretError>;
}

/// Secret parsing or provider failure that omits sensitive input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SecretError {
    /// The reference was empty, oversized, or contained a control character.
    #[error("secret reference is invalid")]
    InvalidReference,
    /// Only explicit local keyring and encrypted-file references are accepted.
    #[error("secret reference provider is unsupported")]
    UnsupportedProvider,
    /// Secret material was empty or exceeded the in-memory bound.
    #[error("resolved secret value is invalid")]
    InvalidValue,
    /// The local provider could not resolve the reference.
    #[error("local secret provider failed")]
    ProviderFailed,
}

/// Highest-precedence values supplied by the CLI parser.
#[derive(Clone, Debug, Default)]
pub struct ConfigOverrides {
    /// Local data root.
    pub data_dir: Option<PathBuf>,
    /// Requested products.
    pub products: Option<Vec<String>>,
    /// Market-price freshness threshold in milliseconds.
    pub stale_after_ms: Option<u64>,
    /// Bounded raw-capture queue capacity.
    pub capture_queue_capacity: Option<usize>,
    /// Total fixed, resident-generation, and queued-record capture memory ceiling.
    pub capture_memory_ceiling_bytes: Option<usize>,
    /// Process-wide destination-registry memory ceiling.
    pub capture_destination_registry_memory_ceiling_bytes: Option<usize>,
    /// Enables paper bot behavior only; live execution is not implied.
    pub paper_bot_enabled: Option<bool>,
    /// Background capture flush interval in milliseconds.
    pub capture_flush_interval_ms: Option<u64>,
    /// Capture drain deadline in milliseconds.
    pub capture_shutdown_ms: Option<u64>,
    /// Source-supervisor shutdown deadline in milliseconds.
    pub source_shutdown_ms: Option<u64>,
    /// Redacted secret locator.
    pub source_secret: Option<SecretReference>,
    /// Complete validated production Coinbase source profile.
    pub coinbase: Option<CoinbaseSourceConfig>,
    /// Complete validated production Kraken book-v2 source profile.
    pub kraken: Option<KrakenSourceConfig>,
}

/// Explicit input set for deterministic configuration loading.
pub struct ConfigSources<'a> {
    config_file: Option<&'a Path>,
    environment: &'a BTreeMap<OsString, OsString>,
    cli: ConfigOverrides,
}

impl fmt::Debug for ConfigSources<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigSources")
            .field("config_file", &self.config_file)
            .field("environment", &"[ENVIRONMENT OMITTED]")
            .field("cli", &self.cli)
            .finish()
    }
}

impl<'a> ConfigSources<'a> {
    /// Constructs precedence inputs without reading ambient process state.
    pub fn new(
        config_file: Option<&'a Path>,
        environment: &'a BTreeMap<OsString, OsString>,
        cli: ConfigOverrides,
    ) -> Self {
        Self {
            config_file,
            environment,
            cli,
        }
    }

    /// Captures process environment once at the application boundary.
    ///
    /// Tests should pass an explicit map through [`Self::new`] to avoid global environment races.
    pub fn process_environment() -> BTreeMap<OsString, OsString> {
        std::env::vars_os().collect()
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    data_dir: Option<PathBuf>,
    products: Option<Vec<String>>,
    stale_after_ms: Option<u64>,
    capture_queue_capacity: Option<usize>,
    capture_memory_ceiling_bytes: Option<usize>,
    capture_destination_registry_memory_ceiling_bytes: Option<usize>,
    paper_bot_enabled: Option<bool>,
    capture_flush_interval_ms: Option<u64>,
    capture_shutdown_ms: Option<u64>,
    source_shutdown_ms: Option<u64>,
    source_secret: Option<String>,
    coinbase: Option<CoinbaseSourceConfig>,
    kraken: Option<KrakenSourceConfig>,
}

/// Validated effective local configuration.
#[derive(Clone)]
pub struct AppConfig {
    data_dir: PathBuf,
    products: Vec<String>,
    stale_after: Duration,
    capture_queue_capacity: NonZeroUsize,
    capture_memory_ceiling_bytes: NonZeroUsize,
    capture_destination_registry_memory_ceiling_bytes: NonZeroUsize,
    paper_bot_enabled: bool,
    capture_flush_interval: Duration,
    capture_shutdown: Duration,
    source_shutdown: Duration,
    source_secret: Option<SecretReference>,
    coinbase: Option<CoinbaseSourceConfig>,
    kraken: Option<KrakenSourceConfig>,
}

impl fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppConfig")
            .field("data_dir", &self.data_dir)
            .field("products", &self.products)
            .field("stale_after", &self.stale_after)
            .field("capture_queue_capacity", &self.capture_queue_capacity)
            .field(
                "capture_memory_ceiling_bytes",
                &self.capture_memory_ceiling_bytes,
            )
            .field(
                "capture_destination_registry_memory_ceiling_bytes",
                &self.capture_destination_registry_memory_ceiling_bytes,
            )
            .field("paper_bot_enabled", &self.paper_bot_enabled)
            .field("capture_flush_interval", &self.capture_flush_interval)
            .field("capture_shutdown", &self.capture_shutdown)
            .field("source_shutdown", &self.source_shutdown)
            .field(
                "source_secret",
                &self.source_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("coinbase", &self.coinbase)
            .field("kraken", &self.kraken)
            .finish()
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(".market-squawk"),
            products: vec!["BTC-USD".to_owned()],
            stale_after: Duration::from_millis(DEFAULT_STALE_AFTER_MS),
            capture_queue_capacity: match NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY) {
                Some(value) => value,
                None => NonZeroUsize::MIN,
            },
            capture_memory_ceiling_bytes: match NonZeroUsize::new(
                DEFAULT_CAPTURE_MEMORY_CEILING_BYTES,
            ) {
                Some(value) => value,
                None => NonZeroUsize::MIN,
            },
            capture_destination_registry_memory_ceiling_bytes: match NonZeroUsize::new(
                DEFAULT_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES,
            ) {
                Some(value) => value,
                None => NonZeroUsize::MIN,
            },
            paper_bot_enabled: false,
            capture_flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
            capture_shutdown: Duration::from_millis(DEFAULT_SHUTDOWN_MS),
            source_shutdown: Duration::from_millis(DEFAULT_SOURCE_SHUTDOWN_MS),
            source_secret: None,
            coinbase: None,
            kraken: None,
        }
    }
}

impl AppConfig {
    /// Loads and validates defaults, file, supplied environment, then CLI overrides.
    pub fn load(sources: ConfigSources<'_>) -> Result<Self, ConfigError> {
        let mut values = ConfigOverrides::from(Self::default());
        if let Some(path) = sources.config_file {
            let file =
                std::fs::File::open(path).map_err(|source| ConfigError::FileIo { source })?;
            let mut bytes = Vec::new();
            file.take(MAX_CONFIG_FILE_BYTES.saturating_add(1))
                .read_to_end(&mut bytes)
                .map_err(|source| ConfigError::FileIo { source })?;
            if u64::try_from(bytes.len()).map_or(true, |length| length > MAX_CONFIG_FILE_BYTES) {
                return Err(ConfigError::FileTooLarge);
            }
            let text = String::from_utf8(bytes).map_err(|_source| ConfigError::FileParse)?;
            let file =
                toml::from_str::<FileConfig>(&text).map_err(|_source| ConfigError::FileParse)?;
            values.apply_file(file)?;
        }
        values.apply_environment(sources.environment)?;
        values.apply(sources.cli);
        Self::try_from(values)
    }

    /// Returns the local data root.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the validated nonempty product list.
    pub fn products(&self) -> &[String] {
        &self.products
    }

    /// Returns the market-price freshness threshold.
    pub const fn stale_after(&self) -> Duration {
        self.stale_after
    }

    /// Returns the bounded raw-capture queue capacity.
    pub const fn capture_queue_capacity(&self) -> NonZeroUsize {
        self.capture_queue_capacity
    }

    /// Returns the unified per-channel capture memory ceiling.
    pub const fn capture_memory_ceiling_bytes(&self) -> NonZeroUsize {
        self.capture_memory_ceiling_bytes
    }

    /// Returns the process-wide capture destination-registry memory ceiling.
    pub const fn capture_destination_registry_memory_ceiling_bytes(&self) -> NonZeroUsize {
        self.capture_destination_registry_memory_ceiling_bytes
    }

    /// Returns whether paper-only bot behavior is enabled.
    pub const fn paper_bot_enabled(&self) -> bool {
        self.paper_bot_enabled
    }

    /// Returns the durable capture flush interval.
    pub const fn capture_flush_interval(&self) -> Duration {
        self.capture_flush_interval
    }

    /// Returns the bounded capture shutdown deadline.
    pub const fn capture_shutdown(&self) -> Duration {
        self.capture_shutdown
    }

    /// Returns the independent bounded source-supervisor shutdown deadline.
    pub const fn source_shutdown(&self) -> Duration {
        self.source_shutdown
    }

    /// Returns the optional redacted source-secret reference.
    pub const fn source_secret(&self) -> Option<&SecretReference> {
        self.source_secret.as_ref()
    }

    /// Returns the optional strict production Coinbase source profile.
    pub const fn coinbase(&self) -> Option<&CoinbaseSourceConfig> {
        self.coinbase.as_ref()
    }

    /// Returns the optional strict production Kraken book-v2 source profile.
    pub const fn kraken(&self) -> Option<&KrakenSourceConfig> {
        self.kraken.as_ref()
    }
}

impl From<AppConfig> for ConfigOverrides {
    fn from(config: AppConfig) -> Self {
        Self {
            data_dir: Some(config.data_dir),
            products: Some(config.products),
            stale_after_ms: Some(duration_millis(config.stale_after)),
            capture_queue_capacity: Some(config.capture_queue_capacity.get()),
            capture_memory_ceiling_bytes: Some(config.capture_memory_ceiling_bytes.get()),
            capture_destination_registry_memory_ceiling_bytes: Some(
                config
                    .capture_destination_registry_memory_ceiling_bytes
                    .get(),
            ),
            paper_bot_enabled: Some(config.paper_bot_enabled),
            capture_flush_interval_ms: Some(duration_millis(config.capture_flush_interval)),
            capture_shutdown_ms: Some(duration_millis(config.capture_shutdown)),
            source_shutdown_ms: Some(duration_millis(config.source_shutdown)),
            source_secret: config.source_secret,
            coinbase: config.coinbase,
            kraken: config.kraken,
        }
    }
}

fn duration_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        return u64::MAX;
    }
    millis as u64
}

impl ConfigOverrides {
    fn apply(&mut self, higher: Self) {
        if higher.data_dir.is_some() {
            self.data_dir = higher.data_dir;
        }
        if higher.products.is_some() {
            self.products = higher.products;
        }
        if higher.stale_after_ms.is_some() {
            self.stale_after_ms = higher.stale_after_ms;
        }
        if higher.capture_queue_capacity.is_some() {
            self.capture_queue_capacity = higher.capture_queue_capacity;
        }
        if higher.capture_memory_ceiling_bytes.is_some() {
            self.capture_memory_ceiling_bytes = higher.capture_memory_ceiling_bytes;
        }
        if higher
            .capture_destination_registry_memory_ceiling_bytes
            .is_some()
        {
            self.capture_destination_registry_memory_ceiling_bytes =
                higher.capture_destination_registry_memory_ceiling_bytes;
        }
        if higher.paper_bot_enabled.is_some() {
            self.paper_bot_enabled = higher.paper_bot_enabled;
        }
        if higher.capture_flush_interval_ms.is_some() {
            self.capture_flush_interval_ms = higher.capture_flush_interval_ms;
        }
        if higher.capture_shutdown_ms.is_some() {
            self.capture_shutdown_ms = higher.capture_shutdown_ms;
        }
        if higher.source_shutdown_ms.is_some() {
            self.source_shutdown_ms = higher.source_shutdown_ms;
        }
        if higher.source_secret.is_some() {
            self.source_secret = higher.source_secret;
        }
        if higher.coinbase.is_some() {
            self.coinbase = higher.coinbase;
        }
        if higher.kraken.is_some() {
            self.kraken = higher.kraken;
        }
    }

    fn apply_file(&mut self, file: FileConfig) -> Result<(), ConfigError> {
        self.apply(Self {
            data_dir: file.data_dir,
            products: file.products,
            stale_after_ms: file.stale_after_ms,
            capture_queue_capacity: file.capture_queue_capacity,
            capture_memory_ceiling_bytes: file.capture_memory_ceiling_bytes,
            capture_destination_registry_memory_ceiling_bytes: file
                .capture_destination_registry_memory_ceiling_bytes,
            paper_bot_enabled: file.paper_bot_enabled,
            capture_flush_interval_ms: file.capture_flush_interval_ms,
            capture_shutdown_ms: file.capture_shutdown_ms,
            source_shutdown_ms: file.source_shutdown_ms,
            source_secret: file
                .source_secret
                .as_deref()
                .map(SecretReference::try_from)
                .transpose()?,
            coinbase: file.coinbase,
            kraken: file.kraken,
        });
        Ok(())
    }

    fn apply_environment(
        &mut self,
        environment: &BTreeMap<OsString, OsString>,
    ) -> Result<(), ConfigError> {
        let mut layer = Self::default();
        for (key, value) in environment {
            let Some(key) = key.to_str() else {
                if non_utf8_key_has_market_squawk_prefix(key) {
                    return Err(ConfigError::NonUtf8Environment);
                }
                continue;
            };
            if !key.starts_with(ENV_PREFIX) {
                continue;
            }
            let value = value.to_str().ok_or(ConfigError::NonUtf8Environment)?;
            match key {
                "MARKET_SQUAWK_DATA_DIR" => layer.data_dir = Some(PathBuf::from(value)),
                "MARKET_SQUAWK_PRODUCTS" => {
                    layer.products = Some(value.split(',').map(str::to_owned).collect());
                }
                "MARKET_SQUAWK_STALE_AFTER_MS" => {
                    layer.stale_after_ms = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY" => {
                    layer.capture_queue_capacity = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES" => {
                    layer.capture_memory_ceiling_bytes = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES" => {
                    layer.capture_destination_registry_memory_ceiling_bytes =
                        Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_PAPER_BOT_ENABLED" => {
                    layer.paper_bot_enabled = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_CAPTURE_FLUSH_INTERVAL_MS" => {
                    layer.capture_flush_interval_ms = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_CAPTURE_SHUTDOWN_MS" => {
                    layer.capture_shutdown_ms = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_SOURCE_SHUTDOWN_MS" => {
                    layer.source_shutdown_ms = Some(parse_environment(value)?);
                }
                "MARKET_SQUAWK_SOURCE_SECRET" => {
                    layer.source_secret = Some(SecretReference::try_from(value)?);
                }
                "MARKET_SQUAWK_COINBASE_JSON" => {
                    layer.coinbase = Some(instruments::parse_environment_profile(value)?);
                }
                "MARKET_SQUAWK_KRAKEN_JSON" => {
                    layer.kraken = Some(instruments::parse_kraken_environment_profile(value)?);
                }
                _ => return Err(ConfigError::UnknownEnvironmentKey),
            }
        }
        self.apply(layer);
        Ok(())
    }
}

#[cfg(unix)]
fn non_utf8_key_has_market_squawk_prefix(key: &std::ffi::OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;

    key.as_bytes().starts_with(ENV_PREFIX.as_bytes())
}

#[cfg(not(unix))]
fn non_utf8_key_has_market_squawk_prefix(_key: &std::ffi::OsStr) -> bool {
    false
}

fn parse_environment<T: std::str::FromStr>(value: &str) -> Result<T, ConfigError> {
    value
        .parse::<T>()
        .map_err(|_source| ConfigError::InvalidEnvironmentValue)
}

impl TryFrom<ConfigOverrides> for AppConfig {
    type Error = ConfigError;

    fn try_from(values: ConfigOverrides) -> Result<Self, Self::Error> {
        let data_dir = values.data_dir.ok_or(ConfigError::InternalComposition)?;
        if data_dir.as_os_str().is_empty() {
            return Err(ConfigError::InvalidDataDirectory);
        }
        let products = values.products.ok_or(ConfigError::InternalComposition)?;
        instruments::validate_product_list(&products)?;
        let stale_after_ms = values
            .stale_after_ms
            .and_then(NonZeroU64::new)
            .ok_or(ConfigError::InvalidStaleAfter)?;
        if !(MIN_STALE_AFTER_MS..=MAX_STALE_AFTER_MS).contains(&stale_after_ms.get()) {
            return Err(ConfigError::InvalidStaleAfter);
        }
        let queue = values
            .capture_queue_capacity
            .and_then(NonZeroUsize::new)
            .ok_or(ConfigError::InvalidQueueCapacity)?;
        if queue.get() > MAX_QUEUE_CAPACITY {
            return Err(ConfigError::InvalidQueueCapacity);
        }
        let capture_memory_ceiling_bytes = values
            .capture_memory_ceiling_bytes
            .and_then(NonZeroUsize::new)
            .filter(|value| value.get() <= MAX_CAPTURE_MEMORY_CEILING_BYTES)
            .ok_or(ConfigError::InvalidCaptureMemoryCeiling)?;
        let capture_destination_registry_memory_ceiling_bytes = values
            .capture_destination_registry_memory_ceiling_bytes
            .and_then(NonZeroUsize::new)
            .filter(|value| value.get() <= MAX_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES)
            .ok_or(ConfigError::InvalidCaptureDestinationRegistryMemoryCeiling)?;
        let flush_ms = values
            .capture_flush_interval_ms
            .and_then(NonZeroU64::new)
            .ok_or(ConfigError::InvalidCaptureTiming)?;
        let shutdown_ms = values
            .capture_shutdown_ms
            .and_then(NonZeroU64::new)
            .ok_or(ConfigError::InvalidCaptureTiming)?;
        if flush_ms > shutdown_ms || shutdown_ms.get() > MAX_SHUTDOWN_MS {
            return Err(ConfigError::InvalidCaptureTiming);
        }
        let source_shutdown_ms = values
            .source_shutdown_ms
            .and_then(NonZeroU64::new)
            .ok_or(ConfigError::InvalidSourceShutdownTiming)?;
        if source_shutdown_ms.get() > MAX_SOURCE_SHUTDOWN_MS {
            return Err(ConfigError::InvalidSourceShutdownTiming);
        }
        Ok(Self {
            data_dir,
            products,
            stale_after: Duration::from_millis(stale_after_ms.get()),
            capture_queue_capacity: queue,
            capture_memory_ceiling_bytes,
            capture_destination_registry_memory_ceiling_bytes,
            paper_bot_enabled: values
                .paper_bot_enabled
                .ok_or(ConfigError::InternalComposition)?,
            capture_flush_interval: Duration::from_millis(flush_ms.get()),
            capture_shutdown: Duration::from_millis(shutdown_ms.get()),
            source_shutdown: Duration::from_millis(source_shutdown_ms.get()),
            source_secret: values.source_secret,
            coinbase: values.coinbase,
            kraken: values.kraken,
        })
    }
}

/// Configuration loading failure with sensitive values omitted from every representation.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The configured file could not be read.
    #[error("failed to read local configuration file")]
    FileIo {
        /// Underlying I/O failure without file content.
        #[source]
        source: std::io::Error,
    },
    /// TOML was malformed or contained an unknown field; input text is intentionally omitted.
    #[error("local configuration file is invalid")]
    FileParse,
    /// The local configuration exceeded the explicit parser input ceiling.
    #[error("local configuration file exceeds the maximum size")]
    FileTooLarge,
    /// A Market Squawk environment key was unknown.
    #[error("unknown MARKET_SQUAWK environment key")]
    UnknownEnvironmentKey,
    /// An in-scope environment key or value was not UTF-8.
    #[error("MARKET_SQUAWK environment key or value is not UTF-8")]
    NonUtf8Environment,
    /// An environment value could not be parsed; the value is intentionally omitted.
    #[error("MARKET_SQUAWK environment value is invalid")]
    InvalidEnvironmentValue,
    /// The local data root was empty.
    #[error("data directory is invalid")]
    InvalidDataDirectory,
    /// The product list was empty, duplicated, oversized, or syntactically invalid.
    #[error("product configuration is invalid")]
    InvalidProducts,
    /// Freshness was zero or outside supported limits.
    #[error("stale-after duration is invalid")]
    InvalidStaleAfter,
    /// Capture queue capacity was zero or outside supported limits.
    #[error("capture queue capacity is invalid")]
    InvalidQueueCapacity,
    /// The unified capture memory ceiling was zero or exceeded the supported bound.
    #[error("capture memory ceiling is invalid")]
    InvalidCaptureMemoryCeiling,
    /// The process destination-registry memory ceiling was zero or exceeded its bound.
    #[error("capture destination registry memory ceiling is invalid")]
    InvalidCaptureDestinationRegistryMemoryCeiling,
    /// Flush/shutdown values were zero, out of order, or outside supported limits.
    #[error("capture flush and shutdown timing is invalid")]
    InvalidCaptureTiming,
    /// Source-supervisor shutdown was zero or outside supported limits.
    #[error("source shutdown timing is invalid")]
    InvalidSourceShutdownTiming,
    /// A redacted secret reference was invalid.
    #[error(transparent)]
    Secret(#[from] SecretError),
    /// A required default was accidentally omitted by internal composition.
    #[error("configuration composition invariant failed")]
    InternalComposition,
}
