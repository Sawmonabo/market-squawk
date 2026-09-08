//! Strict parsing for the one-time provider credential input bundle.

use std::{fmt, fs::File, io::Read as _, path::Path};

use zeroize::Zeroizing;

/// Exact schema admitted by the provider credential bundle parser.
pub const PROVIDER_CREDENTIAL_BUNDLE_SCHEMA: &str = "market-squawk-provider-credentials/v1";

const MAX_FILE_BYTES: usize = 64 * 1_024;
const MAX_FILE_READ_BYTES: u64 = 65_537;
const MAX_LINE_BYTES: usize = 8 * 1_024;
const MAX_KEY_BYTES: usize = 64;
const MAX_VALUE_BYTES: usize = 4 * 1_024;
const FIELD_COUNT: usize = 32;

const SCHEMA: usize = 0;
const SCHWAB_ENABLED: usize = 1;
const SCHWAB_APP_KEY: usize = 2;
const SCHWAB_APP_SECRET: usize = 3;
const ALPACA_ENABLED: usize = 4;
const ALPACA_KEY_ID: usize = 5;
const ALPACA_SECRET_KEY: usize = 6;
const ALPACA_TRADING_API_ENVIRONMENT: usize = 7;
const YAHOO_FINANCE_EXPERIMENTAL_ENABLED: usize = 8;
const YAHOO_FINANCE_EXPERIMENTAL_BROAD_WARM_ENABLED: usize = 9;
const NASDAQ_TRADER_REFERENCE_ENABLED: usize = 10;
const OCC_OPTIONS_REFERENCE_ENABLED: usize = 11;
const CBOE_OPTIONS_REFERENCE_ENABLED: usize = 12;
const IEX_HIST_ENABLED: usize = 13;
const BLS_ENABLED: usize = 14;
const BLS_REGISTRATION_KEY: usize = 15;
const BEA_ENABLED: usize = 16;
const BEA_USER_ID: usize = 17;
const CENSUS_ENABLED: usize = 18;
const CENSUS_API_KEY: usize = 19;
const EIA_ENABLED: usize = 20;
const EIA_API_KEY: usize = 21;
const FRED_ENABLED: usize = 22;
const FRED_API_KEY: usize = 23;
const TIINGO_ENABLED: usize = 24;
const TIINGO_API_TOKEN: usize = 25;
const SEC_ENABLED: usize = 26;
const SEC_USER_AGENT_ORGANIZATION: usize = 27;
const SEC_USER_AGENT_EMAIL: usize = 28;
const TREASURY_FISCAL_DATA_ENABLED: usize = 29;
const TREASURY_DAILY_RATES_ENABLED: usize = 30;
const FEDERAL_RESERVE_BOARD_DIRECT_ENABLED: usize = 31;

#[derive(Clone, Copy)]
enum ValueSyntax {
    Boolean,
    Quoted,
}

#[derive(Clone, Copy)]
struct FieldSpec {
    key: &'static str,
    syntax: ValueSyntax,
}

const FIELD_SPECS: [FieldSpec; FIELD_COUNT] = [
    FieldSpec {
        key: "MARKET_SQUAWK_PROVIDER_CREDENTIAL_SCHEMA",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "SCHWAB_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "SCHWAB_APP_KEY",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "SCHWAB_APP_SECRET",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "ALPACA_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "ALPACA_KEY_ID",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "ALPACA_SECRET_KEY",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "ALPACA_TRADING_API_ENVIRONMENT",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "YAHOO_FINANCE_EXPERIMENTAL_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "YAHOO_FINANCE_EXPERIMENTAL_BROAD_WARM_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "NASDAQ_TRADER_REFERENCE_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "OCC_OPTIONS_REFERENCE_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "CBOE_OPTIONS_REFERENCE_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "IEX_HIST_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "BLS_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "BLS_REGISTRATION_KEY",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "BEA_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "BEA_USER_ID",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "CENSUS_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "CENSUS_API_KEY",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "EIA_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "EIA_API_KEY",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "FRED_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "FRED_API_KEY",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "TIINGO_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "TIINGO_API_TOKEN",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "SEC_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "SEC_USER_AGENT_ORGANIZATION",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "SEC_USER_AGENT_EMAIL",
        syntax: ValueSyntax::Quoted,
    },
    FieldSpec {
        key: "TREASURY_FISCAL_DATA_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "TREASURY_DAILY_RATES_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
    FieldSpec {
        key: "FEDERAL_RESERVE_BOARD_DIRECT_ENABLED",
        syntax: ValueSyntax::Boolean,
    },
];

/// Exact Alpaca Trading API realm admitted by the credential-only V1 contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlpacaCredentialRealm {
    /// Paper Only / Basic credentials; live brokerage credentials are not admitted.
    Paper,
}

impl AlpacaCredentialRealm {
    /// Returns the stable contract spelling for this realm.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
        }
    }
}

/// Code-owned provider enablement and non-secret realm configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderCredentialConfiguration {
    /// Requests the optional read-only Schwab onboarding flow.
    pub schwab_enabled: bool,
    /// Requests Alpaca Paper Only / Basic import and probing.
    pub alpaca_enabled: bool,
    /// Exact Alpaca realm; V1 admits only [`AlpacaCredentialRealm::Paper`].
    pub alpaca_trading_api_environment: AlpacaCredentialRealm,
    /// Requests adaptive, explicit-demand Yahoo Finance enrichment.
    pub yahoo_finance_experimental_enabled: bool,
    /// Requests the separately represented experimental broad-WARM intent.
    pub yahoo_finance_experimental_broad_warm_enabled: bool,
    /// Requests Nasdaq Trader reference ingestion eligibility.
    pub nasdaq_trader_reference_enabled: bool,
    /// Requests OCC options reference ingestion eligibility.
    pub occ_options_reference_enabled: bool,
    /// Requests Cboe options reference ingestion eligibility.
    pub cboe_options_reference_enabled: bool,
    /// Requests exact feed/date IEX HIST job eligibility.
    pub iex_hist_enabled: bool,
    /// Requests BLS registered API V2 import and probing.
    pub bls_enabled: bool,
    /// Requests BEA import and probing.
    pub bea_enabled: bool,
    /// Requests Census Data API import and probing.
    pub census_enabled: bool,
    /// Requests EIA API V2 import and probing.
    pub eia_enabled: bool,
    /// Requests FRED/ALFRED import and probing.
    pub fred_enabled: bool,
    /// Requests optional Tiingo Starter import and probing.
    pub tiingo_enabled: bool,
    /// Requests SEC source onboarding with the supplied identifying values.
    pub sec_enabled: bool,
    /// Requests Treasury Fiscal Data eligibility.
    pub treasury_fiscal_data_enabled: bool,
    /// Requests Treasury daily-rates eligibility.
    pub treasury_daily_rates_enabled: bool,
    /// Requests direct Federal Reserve Board release eligibility.
    pub federal_reserve_board_direct_enabled: bool,
}

/// One credential-bearing value with zeroizing storage and redacted formatting.
pub struct ProviderCredentialValue(Zeroizing<String>);

impl ProviderCredentialValue {
    /// Borrows the value for immediate delegation to an existing protected-secret-store flow.
    ///
    /// The returned value must never enter logs, errors, receipts, or diagnostics.
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    /// Reports whether a disabled provider supplied the required empty value.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for ProviderCredentialValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderCredentialValue([REDACTED])")
    }
}

/// Credential and identifying values, kept separate from enablement configuration.
pub struct ProviderCredentialValues {
    /// Schwab application key.
    pub schwab_app_key: ProviderCredentialValue,
    /// Schwab application secret.
    pub schwab_app_secret: ProviderCredentialValue,
    /// Alpaca Paper key identifier.
    pub alpaca_key_id: ProviderCredentialValue,
    /// Alpaca Paper secret key.
    pub alpaca_secret_key: ProviderCredentialValue,
    /// BLS registered API V2 registration key.
    pub bls_registration_key: ProviderCredentialValue,
    /// BEA API user identifier.
    pub bea_user_id: ProviderCredentialValue,
    /// Census Data API key.
    pub census_api_key: ProviderCredentialValue,
    /// EIA API V2 key.
    pub eia_api_key: ProviderCredentialValue,
    /// FRED/ALFRED API key.
    pub fred_api_key: ProviderCredentialValue,
    /// Optional Tiingo Starter API token.
    pub tiingo_api_token: ProviderCredentialValue,
    /// Truthful SEC identifying organization or owner name.
    pub sec_user_agent_organization: ProviderCredentialValue,
    /// Monitored SEC identifying contact email.
    pub sec_user_agent_email: ProviderCredentialValue,
}

impl fmt::Debug for ProviderCredentialValues {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderCredentialValues([REDACTED])")
    }
}

/// Parsed provider credential bundle with configuration and secret values kept distinct.
pub struct ProviderCredentialBundle {
    /// Provider enablement intent and the exact Alpaca realm.
    pub configuration: ProviderCredentialConfiguration,
    /// Zeroizing values for delegation to the existing provider onboarding and secret store.
    pub credentials: ProviderCredentialValues,
}

impl ProviderCredentialBundle {
    /// Returns the exact schema already validated for this bundle.
    pub const fn schema(&self) -> &'static str {
        PROVIDER_CREDENTIAL_BUNDLE_SCHEMA
    }
}

impl fmt::Debug for ProviderCredentialBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCredentialBundle")
            .field("schema", &PROVIDER_CREDENTIAL_BUNDLE_SCHEMA)
            .field("configuration", &self.configuration)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

/// A bounded credential-bundle read or parse failure that never retains input values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderCredentialBundleParseError {
    /// The path could not be opened for reading.
    #[error("provider credential bundle could not be opened")]
    Open,
    /// File metadata could not be read.
    #[error("provider credential bundle metadata could not be read")]
    Metadata,
    /// Only a regular file is admitted.
    #[error("provider credential bundle is not a regular file")]
    NotRegularFile,
    /// The file exceeded the fixed parser byte ceiling.
    #[error("provider credential bundle exceeds the {max_bytes}-byte limit")]
    FileTooLarge {
        /// Code-owned maximum admitted file length.
        max_bytes: usize,
    },
    /// Bounded parser storage could not be reserved.
    #[error("provider credential bundle parser storage is unavailable")]
    Allocation,
    /// The file could not be read completely.
    #[error("provider credential bundle could not be read")]
    Read,
    /// The file was not valid UTF-8.
    #[error("provider credential bundle is not valid UTF-8")]
    InvalidUtf8,
    /// One physical line exceeded the fixed byte ceiling.
    #[error("provider credential bundle line {line} exceeds the {max_bytes}-byte limit")]
    LineTooLong {
        /// One-based physical line number.
        line: usize,
        /// Code-owned maximum admitted line length.
        max_bytes: usize,
    },
    /// A non-comment line was not one exact `KEY=value` assignment.
    #[error("provider credential bundle line {line} is malformed")]
    MalformedLine {
        /// One-based physical line number.
        line: usize,
    },
    /// An input key exceeded the fixed byte ceiling; its bytes are intentionally omitted.
    #[error("provider credential bundle key on line {line} exceeds the {max_bytes}-byte limit")]
    KeyTooLong {
        /// One-based physical line number.
        line: usize,
        /// Code-owned maximum admitted key length.
        max_bytes: usize,
    },
    /// A key outside the closed V1 set was supplied; its bytes are intentionally omitted.
    #[error("provider credential bundle contains an unknown key on line {line}")]
    UnknownKey {
        /// One-based physical line number.
        line: usize,
    },
    /// A code-owned key appeared more than once.
    #[error("provider credential bundle key {key} is duplicated on line {line}")]
    DuplicateKey {
        /// Code-owned key name.
        key: &'static str,
        /// One-based physical line number.
        line: usize,
    },
    /// A quoted field did not have one opening and one final delimiter.
    #[error("provider credential bundle key {key} must use a double-quoted value")]
    InvalidQuotedValue {
        /// Code-owned key name.
        key: &'static str,
    },
    /// A parsed value exceeded the fixed byte ceiling.
    #[error("provider credential bundle key {key} exceeds the {max_bytes}-byte value limit")]
    ValueTooLong {
        /// Code-owned key name.
        key: &'static str,
        /// Code-owned maximum admitted value length.
        max_bytes: usize,
    },
    /// A value contained a control character.
    #[error("provider credential bundle key {key} contains an invalid value")]
    InvalidValue {
        /// Code-owned key name.
        key: &'static str,
    },
    /// A template placeholder was not replaced.
    #[error("provider credential bundle key {key} still contains a placeholder")]
    Placeholder {
        /// Code-owned key name.
        key: &'static str,
    },
    /// A required code-owned key was absent.
    #[error("provider credential bundle is missing key {key}")]
    MissingKey {
        /// Code-owned key name.
        key: &'static str,
    },
    /// All keys existed but did not follow the code-owned V1 order.
    #[error(
        "provider credential bundle key {found} on line {line} is out of order; expected {expected}"
    )]
    OutOfOrderKey {
        /// Code-owned expected key name.
        expected: &'static str,
        /// Code-owned encountered key name.
        found: &'static str,
        /// One-based physical line number.
        line: usize,
    },
    /// The schema value did not equal the exact V1 identifier.
    #[error("provider credential bundle schema is unsupported")]
    InvalidSchema,
    /// A boolean was not exactly unquoted `true` or `false`.
    #[error("provider credential bundle key {key} must be exactly true or false")]
    InvalidBoolean {
        /// Code-owned key name.
        key: &'static str,
    },
    /// The Alpaca realm was not exactly quoted `paper`.
    #[error("provider credential bundle Alpaca Trading API environment must be paper")]
    InvalidAlpacaRealm,
    /// An enabled provider omitted one of its credential values.
    #[error("enabled provider credential bundle key {key} must not be empty")]
    RequiredValue {
        /// Code-owned credential key name.
        key: &'static str,
    },
    /// A disabled provider retained a value that the V1 contract requires to be empty.
    #[error("disabled provider credential bundle key {key} must be empty")]
    DisabledValue {
        /// Code-owned credential key name.
        key: &'static str,
    },
}

/// Reads and strictly parses one `market-squawk-provider-credentials/v1` file.
///
/// The parser does not source the file, expand variables, interpolate commands, access provider
/// networks, activate providers, or write configuration/secrets. Successful values are returned
/// only for explicit delegation to the existing application-owned onboarding authorities.
///
/// # Errors
///
/// Returns [`ProviderCredentialBundleParseError`] when the file or any part of the closed V1
/// grammar violates its fixed bounds, field set, field order, scalar grammar, or enablement/value
/// relationship.
pub fn parse_provider_credential_bundle_file(
    path: impl AsRef<Path>,
) -> Result<ProviderCredentialBundle, ProviderCredentialBundleParseError> {
    let file =
        File::open(path.as_ref()).map_err(|_error| ProviderCredentialBundleParseError::Open)?;
    let metadata = file
        .metadata()
        .map_err(|_error| ProviderCredentialBundleParseError::Metadata)?;
    if !metadata.is_file() {
        return Err(ProviderCredentialBundleParseError::NotRegularFile);
    }
    if metadata.len() >= MAX_FILE_READ_BYTES {
        return Err(ProviderCredentialBundleParseError::FileTooLarge {
            max_bytes: MAX_FILE_BYTES,
        });
    }

    let mut input = Zeroizing::new(Vec::new());
    input
        .try_reserve_exact(MAX_FILE_BYTES + 1)
        .map_err(|_error| ProviderCredentialBundleParseError::Allocation)?;
    file.take(MAX_FILE_READ_BYTES)
        .read_to_end(&mut input)
        .map_err(|_error| ProviderCredentialBundleParseError::Read)?;
    parse_provider_credential_bundle_bytes(&input)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn parse_provider_credential_bundle_bytes(
    input: &[u8],
) -> Result<ProviderCredentialBundle, ProviderCredentialBundleParseError> {
    if input.len() > MAX_FILE_BYTES {
        return Err(ProviderCredentialBundleParseError::FileTooLarge {
            max_bytes: MAX_FILE_BYTES,
        });
    }
    let text = std::str::from_utf8(input)
        .map_err(|_error| ProviderCredentialBundleParseError::InvalidUtf8)?;
    let mut values: [Option<Zeroizing<String>>; FIELD_COUNT] = std::array::from_fn(|_| None);
    let mut encountered_indices = [usize::MAX; FIELD_COUNT];
    let mut encountered_lines = [0; FIELD_COUNT];
    let mut encountered_count = 0;

    for (line_index, physical_line) in text.split('\n').enumerate() {
        let line_number = line_index + 1;
        let line = physical_line.strip_suffix('\r').unwrap_or(physical_line);
        if line.len() > MAX_LINE_BYTES {
            return Err(ProviderCredentialBundleParseError::LineTooLong {
                line: line_number,
                max_bytes: MAX_LINE_BYTES,
            });
        }
        if line.contains('\r') {
            return Err(ProviderCredentialBundleParseError::MalformedLine { line: line_number });
        }
        let trimmed_start = line.trim_start();
        if trimmed_start.is_empty() || trimmed_start.starts_with('#') {
            continue;
        }

        let Some((input_key, raw_value)) = line.split_once('=') else {
            return Err(ProviderCredentialBundleParseError::MalformedLine { line: line_number });
        };
        if input_key.is_empty() {
            return Err(ProviderCredentialBundleParseError::MalformedLine { line: line_number });
        }
        if input_key.len() > MAX_KEY_BYTES {
            return Err(ProviderCredentialBundleParseError::KeyTooLong {
                line: line_number,
                max_bytes: MAX_KEY_BYTES,
            });
        }
        let Some((field_index, spec)) = FIELD_SPECS
            .iter()
            .enumerate()
            .find(|(_index, candidate)| candidate.key == input_key)
        else {
            return Err(ProviderCredentialBundleParseError::UnknownKey { line: line_number });
        };
        let field_slot = values
            .get_mut(field_index)
            .ok_or(ProviderCredentialBundleParseError::UnknownKey { line: line_number })?;
        if field_slot.is_some() {
            return Err(ProviderCredentialBundleParseError::DuplicateKey {
                key: spec.key,
                line: line_number,
            });
        }

        let value = match spec.syntax {
            ValueSyntax::Boolean => raw_value,
            ValueSyntax::Quoted => raw_value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .ok_or(ProviderCredentialBundleParseError::InvalidQuotedValue { key: spec.key })?,
        };
        if value.len() > MAX_VALUE_BYTES {
            return Err(ProviderCredentialBundleParseError::ValueTooLong {
                key: spec.key,
                max_bytes: MAX_VALUE_BYTES,
            });
        }
        if value.chars().any(char::is_control)
            || matches!(spec.syntax, ValueSyntax::Quoted) && value.contains('"')
        {
            return Err(ProviderCredentialBundleParseError::InvalidValue { key: spec.key });
        }
        if is_placeholder(value) {
            return Err(ProviderCredentialBundleParseError::Placeholder { key: spec.key });
        }

        let mut owned = Zeroizing::new(String::new());
        owned
            .try_reserve_exact(value.len())
            .map_err(|_error| ProviderCredentialBundleParseError::Allocation)?;
        owned.push_str(value);
        *field_slot = Some(owned);

        let encountered_slot = encountered_indices
            .get_mut(encountered_count)
            .ok_or(ProviderCredentialBundleParseError::UnknownKey { line: line_number })?;
        *encountered_slot = field_index;
        let encountered_line = encountered_lines
            .get_mut(encountered_count)
            .ok_or(ProviderCredentialBundleParseError::UnknownKey { line: line_number })?;
        *encountered_line = line_number;
        encountered_count += 1;
    }

    for (index, spec) in FIELD_SPECS.iter().enumerate() {
        if values.get(index).is_none_or(Option::is_none) {
            return Err(ProviderCredentialBundleParseError::MissingKey { key: spec.key });
        }
    }
    for (position, actual_index) in encountered_indices.iter().copied().enumerate() {
        if position != actual_index {
            let expected = FIELD_SPECS
                .get(position)
                .ok_or(ProviderCredentialBundleParseError::Allocation)?;
            let found = FIELD_SPECS
                .get(actual_index)
                .ok_or(ProviderCredentialBundleParseError::Allocation)?;
            let line = encountered_lines
                .get(position)
                .copied()
                .ok_or(ProviderCredentialBundleParseError::Allocation)?;
            return Err(ProviderCredentialBundleParseError::OutOfOrderKey {
                expected: expected.key,
                found: found.key,
                line,
            });
        }
    }

    if field_value(&values, SCHEMA)? != PROVIDER_CREDENTIAL_BUNDLE_SCHEMA {
        return Err(ProviderCredentialBundleParseError::InvalidSchema);
    }
    let configuration = ProviderCredentialConfiguration {
        schwab_enabled: boolean_field(&values, SCHWAB_ENABLED)?,
        alpaca_enabled: boolean_field(&values, ALPACA_ENABLED)?,
        alpaca_trading_api_environment: alpaca_realm_field(
            &values,
            ALPACA_TRADING_API_ENVIRONMENT,
        )?,
        yahoo_finance_experimental_enabled: boolean_field(
            &values,
            YAHOO_FINANCE_EXPERIMENTAL_ENABLED,
        )?,
        yahoo_finance_experimental_broad_warm_enabled: boolean_field(
            &values,
            YAHOO_FINANCE_EXPERIMENTAL_BROAD_WARM_ENABLED,
        )?,
        nasdaq_trader_reference_enabled: boolean_field(&values, NASDAQ_TRADER_REFERENCE_ENABLED)?,
        occ_options_reference_enabled: boolean_field(&values, OCC_OPTIONS_REFERENCE_ENABLED)?,
        cboe_options_reference_enabled: boolean_field(&values, CBOE_OPTIONS_REFERENCE_ENABLED)?,
        iex_hist_enabled: boolean_field(&values, IEX_HIST_ENABLED)?,
        bls_enabled: boolean_field(&values, BLS_ENABLED)?,
        bea_enabled: boolean_field(&values, BEA_ENABLED)?,
        census_enabled: boolean_field(&values, CENSUS_ENABLED)?,
        eia_enabled: boolean_field(&values, EIA_ENABLED)?,
        fred_enabled: boolean_field(&values, FRED_ENABLED)?,
        tiingo_enabled: boolean_field(&values, TIINGO_ENABLED)?,
        sec_enabled: boolean_field(&values, SEC_ENABLED)?,
        treasury_fiscal_data_enabled: boolean_field(&values, TREASURY_FISCAL_DATA_ENABLED)?,
        treasury_daily_rates_enabled: boolean_field(&values, TREASURY_DAILY_RATES_ENABLED)?,
        federal_reserve_board_direct_enabled: boolean_field(
            &values,
            FEDERAL_RESERVE_BOARD_DIRECT_ENABLED,
        )?,
    };

    validate_enabled_values(
        &values,
        configuration.schwab_enabled,
        &[SCHWAB_APP_KEY, SCHWAB_APP_SECRET],
    )?;
    validate_enabled_values(
        &values,
        configuration.alpaca_enabled,
        &[ALPACA_KEY_ID, ALPACA_SECRET_KEY],
    )?;
    validate_enabled_values(&values, configuration.bls_enabled, &[BLS_REGISTRATION_KEY])?;
    validate_enabled_values(&values, configuration.bea_enabled, &[BEA_USER_ID])?;
    validate_enabled_values(&values, configuration.census_enabled, &[CENSUS_API_KEY])?;
    validate_enabled_values(&values, configuration.eia_enabled, &[EIA_API_KEY])?;
    validate_enabled_values(&values, configuration.fred_enabled, &[FRED_API_KEY])?;
    validate_enabled_values(&values, configuration.tiingo_enabled, &[TIINGO_API_TOKEN])?;
    validate_enabled_values(
        &values,
        configuration.sec_enabled,
        &[SEC_USER_AGENT_ORGANIZATION, SEC_USER_AGENT_EMAIL],
    )?;

    let credentials = ProviderCredentialValues {
        schwab_app_key: take_credential(&mut values, SCHWAB_APP_KEY)?,
        schwab_app_secret: take_credential(&mut values, SCHWAB_APP_SECRET)?,
        alpaca_key_id: take_credential(&mut values, ALPACA_KEY_ID)?,
        alpaca_secret_key: take_credential(&mut values, ALPACA_SECRET_KEY)?,
        bls_registration_key: take_credential(&mut values, BLS_REGISTRATION_KEY)?,
        bea_user_id: take_credential(&mut values, BEA_USER_ID)?,
        census_api_key: take_credential(&mut values, CENSUS_API_KEY)?,
        eia_api_key: take_credential(&mut values, EIA_API_KEY)?,
        fred_api_key: take_credential(&mut values, FRED_API_KEY)?,
        tiingo_api_token: take_credential(&mut values, TIINGO_API_TOKEN)?,
        sec_user_agent_organization: take_credential(&mut values, SEC_USER_AGENT_ORGANIZATION)?,
        sec_user_agent_email: take_credential(&mut values, SEC_USER_AGENT_EMAIL)?,
    };

    Ok(ProviderCredentialBundle {
        configuration,
        credentials,
    })
}

fn field_value(
    values: &[Option<Zeroizing<String>>; FIELD_COUNT],
    index: usize,
) -> Result<&str, ProviderCredentialBundleParseError> {
    values
        .get(index)
        .and_then(Option::as_ref)
        .map(|value| value.as_str())
        .ok_or_else(|| ProviderCredentialBundleParseError::MissingKey {
            key: FIELD_SPECS.get(index).map_or("<internal>", |spec| spec.key),
        })
}

fn boolean_field(
    values: &[Option<Zeroizing<String>>; FIELD_COUNT],
    index: usize,
) -> Result<bool, ProviderCredentialBundleParseError> {
    match field_value(values, index)? {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ProviderCredentialBundleParseError::InvalidBoolean {
            key: FIELD_SPECS.get(index).map_or("<internal>", |spec| spec.key),
        }),
    }
}

fn alpaca_realm_field(
    values: &[Option<Zeroizing<String>>; FIELD_COUNT],
    index: usize,
) -> Result<AlpacaCredentialRealm, ProviderCredentialBundleParseError> {
    match field_value(values, index)? {
        "paper" => Ok(AlpacaCredentialRealm::Paper),
        _ => Err(ProviderCredentialBundleParseError::InvalidAlpacaRealm),
    }
}

fn validate_enabled_values(
    values: &[Option<Zeroizing<String>>; FIELD_COUNT],
    enabled: bool,
    indices: &[usize],
) -> Result<(), ProviderCredentialBundleParseError> {
    for &index in indices {
        let value = field_value(values, index)?;
        let key = FIELD_SPECS.get(index).map_or("<internal>", |spec| spec.key);
        if enabled && value.trim().is_empty() {
            return Err(ProviderCredentialBundleParseError::RequiredValue { key });
        }
        if !enabled && !value.is_empty() {
            return Err(ProviderCredentialBundleParseError::DisabledValue { key });
        }
    }
    Ok(())
}

fn take_credential(
    values: &mut [Option<Zeroizing<String>>; FIELD_COUNT],
    index: usize,
) -> Result<ProviderCredentialValue, ProviderCredentialBundleParseError> {
    values
        .get_mut(index)
        .and_then(Option::take)
        .map(ProviderCredentialValue)
        .ok_or_else(|| ProviderCredentialBundleParseError::MissingKey {
            key: FIELD_SPECS.get(index).map_or("<internal>", |spec| spec.key),
        })
}

fn is_placeholder(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() > 2 && trimmed.starts_with('<') && trimmed.ends_with('>')
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    const LITERAL_SECRET: &str = "literal-${HOME}-$(id)-`id`";
    const VALID_BUNDLE: &str = r#"# test-only credential bundle

MARKET_SQUAWK_PROVIDER_CREDENTIAL_SCHEMA="market-squawk-provider-credentials/v1"
SCHWAB_ENABLED=false
SCHWAB_APP_KEY=""
SCHWAB_APP_SECRET=""
ALPACA_ENABLED=true
ALPACA_KEY_ID="test-alpaca-key-id"
ALPACA_SECRET_KEY="literal-${HOME}-$(id)-`id`"
ALPACA_TRADING_API_ENVIRONMENT="paper"
YAHOO_FINANCE_EXPERIMENTAL_ENABLED=true
YAHOO_FINANCE_EXPERIMENTAL_BROAD_WARM_ENABLED=false
NASDAQ_TRADER_REFERENCE_ENABLED=true
OCC_OPTIONS_REFERENCE_ENABLED=true
CBOE_OPTIONS_REFERENCE_ENABLED=true
IEX_HIST_ENABLED=true
BLS_ENABLED=true
BLS_REGISTRATION_KEY="test-bls-key"
BEA_ENABLED=true
BEA_USER_ID="test-bea-user"
CENSUS_ENABLED=true
CENSUS_API_KEY="test-census-key"
EIA_ENABLED=true
EIA_API_KEY="test-eia-key"
FRED_ENABLED=true
FRED_API_KEY="test-fred-key"
TIINGO_ENABLED=false
TIINGO_API_TOKEN=""
SEC_ENABLED=true
SEC_USER_AGENT_ORGANIZATION="Test Organization"
SEC_USER_AGENT_EMAIL="operator@example.invalid"
TREASURY_FISCAL_DATA_ENABLED=true
TREASURY_DAILY_RATES_ENABLED=true
FEDERAL_RESERVE_BOARD_DIRECT_ENABLED=true
"#;

    #[test]
    fn parses_crlf_file_literally_and_redacts_credentials() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("credentials.env");
        fs::write(&path, VALID_BUNDLE.replace('\n', "\r\n"))?;

        let bundle = parse_provider_credential_bundle_file(&path)?;

        assert_eq!(bundle.schema(), PROVIDER_CREDENTIAL_BUNDLE_SCHEMA);
        assert!(bundle.configuration.alpaca_enabled);
        assert!(!bundle.configuration.schwab_enabled);
        assert_eq!(
            bundle.configuration.alpaca_trading_api_environment,
            AlpacaCredentialRealm::Paper
        );
        assert_eq!(
            bundle.credentials.alpaca_secret_key.expose_secret(),
            LITERAL_SECRET
        );
        assert!(bundle.credentials.schwab_app_secret.is_empty());
        let rendered = format!("{bundle:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains(LITERAL_SECRET));
        assert!(!format!("{:?}", bundle.credentials).contains("test-fred-key"));
        Ok(())
    }

    #[test]
    fn rejects_unknown_duplicate_missing_and_out_of_order_keys() {
        let unknown = VALID_BUNDLE.replacen("SCHWAB_ENABLED", "SCHWAB_ENABLEDX", 1);
        assert!(matches!(
            parse_provider_credential_bundle_bytes(unknown.as_bytes()),
            Err(ProviderCredentialBundleParseError::UnknownKey { .. })
        ));

        let duplicate = VALID_BUNDLE.replacen(
            "SCHWAB_ENABLED=false",
            "SCHWAB_ENABLED=false\nSCHWAB_ENABLED=false",
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(duplicate.as_bytes()),
            Err(ProviderCredentialBundleParseError::DuplicateKey {
                key: "SCHWAB_ENABLED",
                ..
            })
        ));

        let missing = VALID_BUNDLE.replacen("SCHWAB_ENABLED=false\n", "", 1);
        assert!(matches!(
            parse_provider_credential_bundle_bytes(missing.as_bytes()),
            Err(ProviderCredentialBundleParseError::MissingKey {
                key: "SCHWAB_ENABLED"
            })
        ));

        let out_of_order = VALID_BUNDLE.replacen(
            "SCHWAB_ENABLED=false\nSCHWAB_APP_KEY=\"\"",
            "SCHWAB_APP_KEY=\"\"\nSCHWAB_ENABLED=false",
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(out_of_order.as_bytes()),
            Err(ProviderCredentialBundleParseError::OutOfOrderKey {
                expected: "SCHWAB_ENABLED",
                found: "SCHWAB_APP_KEY",
                ..
            })
        ));
    }

    #[test]
    fn rejects_invalid_scalars_placeholders_and_enablement_mismatches() {
        let schema = VALID_BUNDLE.replace(
            "market-squawk-provider-credentials/v1",
            "market-squawk-provider-credentials/v2",
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(schema.as_bytes()),
            Err(ProviderCredentialBundleParseError::InvalidSchema)
        ));

        let boolean = VALID_BUNDLE.replacen("SCHWAB_ENABLED=false", "SCHWAB_ENABLED=FALSE", 1);
        assert!(matches!(
            parse_provider_credential_bundle_bytes(boolean.as_bytes()),
            Err(ProviderCredentialBundleParseError::InvalidBoolean {
                key: "SCHWAB_ENABLED"
            })
        ));

        let realm = VALID_BUNDLE.replacen(
            "ALPACA_TRADING_API_ENVIRONMENT=\"paper\"",
            "ALPACA_TRADING_API_ENVIRONMENT=\"live\"",
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(realm.as_bytes()),
            Err(ProviderCredentialBundleParseError::InvalidAlpacaRealm)
        ));

        let placeholder = VALID_BUNDLE.replacen(
            "ALPACA_KEY_ID=\"test-alpaca-key-id\"",
            "ALPACA_KEY_ID=\"<ALPACA_KEY_ID>\"",
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(placeholder.as_bytes()),
            Err(ProviderCredentialBundleParseError::Placeholder {
                key: "ALPACA_KEY_ID"
            })
        ));

        let required = VALID_BUNDLE.replacen(
            "ALPACA_KEY_ID=\"test-alpaca-key-id\"",
            "ALPACA_KEY_ID=\"\"",
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(required.as_bytes()),
            Err(ProviderCredentialBundleParseError::RequiredValue {
                key: "ALPACA_KEY_ID"
            })
        ));

        let disabled =
            VALID_BUNDLE.replacen("SCHWAB_APP_KEY=\"\"", "SCHWAB_APP_KEY=\"unused-key\"", 1);
        assert!(matches!(
            parse_provider_credential_bundle_bytes(disabled.as_bytes()),
            Err(ProviderCredentialBundleParseError::DisabledValue {
                key: "SCHWAB_APP_KEY"
            })
        ));

        let interior_quote = VALID_BUNDLE.replacen(
            "ALPACA_KEY_ID=\"test-alpaca-key-id\"",
            "ALPACA_KEY_ID=\"test\"key\"",
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(interior_quote.as_bytes()),
            Err(ProviderCredentialBundleParseError::InvalidValue {
                key: "ALPACA_KEY_ID"
            })
        ));
    }

    #[test]
    fn enforces_file_line_key_and_value_bounds_without_echoing_input() {
        let oversized_file = vec![b'#'; MAX_FILE_BYTES + 1];
        assert!(matches!(
            parse_provider_credential_bundle_bytes(&oversized_file),
            Err(ProviderCredentialBundleParseError::FileTooLarge {
                max_bytes: MAX_FILE_BYTES
            })
        ));

        let oversized_line = format!("#{}\n{VALID_BUNDLE}", "x".repeat(MAX_LINE_BYTES));
        assert!(matches!(
            parse_provider_credential_bundle_bytes(oversized_line.as_bytes()),
            Err(ProviderCredentialBundleParseError::LineTooLong { .. })
        ));

        let oversized_key = format!("{}=false\n{VALID_BUNDLE}", "K".repeat(MAX_KEY_BYTES + 1));
        assert!(matches!(
            parse_provider_credential_bundle_bytes(oversized_key.as_bytes()),
            Err(ProviderCredentialBundleParseError::KeyTooLong { .. })
        ));

        let oversized_value = VALID_BUNDLE.replacen(
            "ALPACA_KEY_ID=\"test-alpaca-key-id\"",
            &format!("ALPACA_KEY_ID=\"{}\"", "x".repeat(MAX_VALUE_BYTES + 1)),
            1,
        );
        assert!(matches!(
            parse_provider_credential_bundle_bytes(oversized_value.as_bytes()),
            Err(ProviderCredentialBundleParseError::ValueTooLong {
                key: "ALPACA_KEY_ID",
                ..
            })
        ));

        let invalid_utf8 = [0xff];
        assert!(matches!(
            parse_provider_credential_bundle_bytes(&invalid_utf8),
            Err(ProviderCredentialBundleParseError::InvalidUtf8)
        ));
    }
}
