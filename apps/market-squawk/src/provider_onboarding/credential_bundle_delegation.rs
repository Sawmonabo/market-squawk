//! Local-only delegation of one parsed provider credential bundle.

use std::sync::Arc;

use market_squawk_platform::SecretValue;
use market_squawk_sources::ProfileReleaseState;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::contracts::{OnboardingNextAction, ProviderProfileView};
use super::credential_bundle::{
    PROVIDER_CREDENTIAL_BUNDLE_SCHEMA, ProviderCredentialBundle, ProviderCredentialConfiguration,
    ProviderCredentialValue, ProviderCredentialValues,
};
use super::service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};

const PROVIDER_COUNT: usize = 17;

const ALPACA_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "alpaca.basic-market-data",
    capability_revision: 3,
    release_state: ProfileReleaseState::Available,
};
const SCHWAB_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "schwab.trader-api-market-data",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const YAHOO_FINANCE_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "yahoo-finance.experimental-enrichment",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const NASDAQ_TRADER_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "nasdaq-trader-symbol-directory-reference",
    capability_revision: 3,
    release_state: ProfileReleaseState::RightsLimited,
};
const OCC_OPTIONS_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "occ.options-reference",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const CBOE_OPTIONS_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "cboe.options-reference",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const IEX_HIST_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "iex.hist-feed-files",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const BLS_REGISTERED_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "bls.v2-registered",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const BEA_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "bea.api-data",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const CENSUS_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "census.data-api",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const EIA_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "eia.api-v2",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const FRED_ALFRED_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "fred-alfred.api-v1-v2",
    capability_revision: 5,
    release_state: ProfileReleaseState::RightsLimited,
};
const SEC_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "sec.edgar-public",
    capability_revision: 4,
    release_state: ProfileReleaseState::Available,
};
const TIINGO_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "tiingo.starter-eod-nav",
    capability_revision: 3,
    release_state: ProfileReleaseState::RefreshRequired,
};
const TREASURY_FISCAL_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "treasury.fiscal-data",
    capability_revision: 4,
    release_state: ProfileReleaseState::Available,
};
const TREASURY_DAILY_RATES_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "treasury.daily-rates-xml",
    capability_revision: 4,
    release_state: ProfileReleaseState::Available,
};
const FEDERAL_RESERVE_BOARD_PROFILE: RegisteredProfileSpec = RegisteredProfileSpec {
    surface_id: "federal-reserve-board.data-download-program",
    capability_revision: 4,
    release_state: ProfileReleaseState::Available,
};

const PROVIDER_SPECS: [ProviderDelegationSpec; PROVIDER_COUNT] = [
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Schwab,
        SCHWAB_PROFILE,
        CredentialDelegationKind::SchwabApplication,
    ),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Alpaca,
        ALPACA_PROFILE,
        CredentialDelegationKind::AlpacaPaper,
    ),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::YahooFinanceExperimental,
        YAHOO_FINANCE_PROFILE,
    ),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::NasdaqTraderReference,
        NASDAQ_TRADER_PROFILE,
    ),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::OccOptionsReference,
        OCC_OPTIONS_PROFILE,
    ),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::CboeOptionsReference,
        CBOE_OPTIONS_PROFILE,
    ),
    ProviderDelegationSpec::no_secret(ProviderCredentialBundleProvider::IexHist, IEX_HIST_PROFILE),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Bls,
        BLS_REGISTERED_PROFILE,
        CredentialDelegationKind::BlsRegistrationKey,
    ),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Bea,
        BEA_PROFILE,
        CredentialDelegationKind::BeaUserId,
    ),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Census,
        CENSUS_PROFILE,
        CredentialDelegationKind::CensusApiKey,
    ),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Eia,
        EIA_PROFILE,
        CredentialDelegationKind::EiaApiKey,
    ),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::FredAlfred,
        FRED_ALFRED_PROFILE,
        CredentialDelegationKind::FredApiKey,
    ),
    ProviderDelegationSpec::credential(
        ProviderCredentialBundleProvider::Tiingo,
        TIINGO_PROFILE,
        CredentialDelegationKind::TiingoApiToken,
    ),
    ProviderDelegationSpec::no_secret(ProviderCredentialBundleProvider::Sec, SEC_PROFILE),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::TreasuryFiscalData,
        TREASURY_FISCAL_PROFILE,
    ),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::TreasuryDailyRates,
        TREASURY_DAILY_RATES_PROFILE,
    ),
    ProviderDelegationSpec::no_secret(
        ProviderCredentialBundleProvider::FederalReserveBoardDirect,
        FEDERAL_RESERVE_BOARD_PROFILE,
    ),
];

/// One provider represented by the exact V1 credential bundle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCredentialBundleProvider {
    /// Optional owner-enabled Charles Schwab market data.
    Schwab,
    /// Alpaca Paper Only / Basic market data.
    Alpaca,
    /// Explicit-demand experimental Yahoo Finance enrichment.
    YahooFinanceExperimental,
    /// Nasdaq Trader current reference files.
    NasdaqTraderReference,
    /// OCC options reference files.
    OccOptionsReference,
    /// Cboe options reference files.
    CboeOptionsReference,
    /// Explicit feed/date IEX HIST jobs.
    IexHist,
    /// Registered BLS API V2.
    Bls,
    /// Bureau of Economic Analysis API.
    Bea,
    /// Census Data API.
    Census,
    /// EIA API V2.
    Eia,
    /// FRED and ALFRED APIs.
    FredAlfred,
    /// Optional Tiingo Starter.
    Tiingo,
    /// SEC EDGAR public APIs.
    Sec,
    /// U.S. Treasury Fiscal Data.
    TreasuryFiscalData,
    /// U.S. Treasury daily-rate XML.
    TreasuryDailyRates,
    /// Federal Reserve Board direct releases.
    FederalReserveBoardDirect,
}

impl ProviderCredentialBundleProvider {
    /// Returns a stable, secret-free provider label for receipts and matching.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schwab => "schwab",
            Self::Alpaca => "alpaca",
            Self::YahooFinanceExperimental => "yahoo_finance_experimental",
            Self::NasdaqTraderReference => "nasdaq_trader_reference",
            Self::OccOptionsReference => "occ_options_reference",
            Self::CboeOptionsReference => "cboe_options_reference",
            Self::IexHist => "iex_hist",
            Self::Bls => "bls",
            Self::Bea => "bea",
            Self::Census => "census",
            Self::Eia => "eia",
            Self::FredAlfred => "fred_alfred",
            Self::Tiingo => "tiingo",
            Self::Sec => "sec",
            Self::TreasuryFiscalData => "treasury_fiscal_data",
            Self::TreasuryDailyRates => "treasury_daily_rates",
            Self::FederalReserveBoardDirect => "federal_reserve_board_direct",
        }
    }
}

/// Why an enabled bundle entry cannot be delegated to the current onboarding service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCredentialProfileUnavailableReason {
    /// No selected built-in onboarding profile is mapped for this V1 provider.
    NoRegisteredSelectedProfile,
    /// The registered profile revision or release gate differs from the audited mapping.
    CapabilityMismatch,
    /// The exact V1 bundle lacks public inputs required by the current registered profile.
    ExactBundleCannotSatisfyProfile,
}

/// Secret-free disposition for one provider after local credential delegation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCredentialDelegationDisposition {
    /// The operator did not request this provider.
    Disabled,
    /// The current application cannot truthfully delegate this requested provider.
    ProfileUnavailable(ProviderCredentialProfileUnavailableReason),
    /// Enabled intent is durable, but provider verification/activation remains required.
    ProbeRequired,
    /// The credential is retained by the existing secret store but remains unverified.
    CredentialImported,
}

/// Secret-free result for one provider in the fixed V1 provider order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderCredentialDelegationResult {
    provider: ProviderCredentialBundleProvider,
    selected_surface_id: Option<&'static str>,
    capability_revision: Option<u64>,
    release_state: Option<ProfileReleaseState>,
    disposition: ProviderCredentialDelegationDisposition,
    session_id: Option<Uuid>,
    next_action: Option<OnboardingNextAction>,
}

impl ProviderCredentialDelegationResult {
    /// Returns the exact provider represented by this result.
    pub const fn provider(&self) -> ProviderCredentialBundleProvider {
        self.provider
    }

    /// Returns the selected built-in surface, or `None` when no mapping exists.
    pub const fn selected_surface_id(&self) -> Option<&'static str> {
        self.selected_surface_id
    }

    /// Returns the exact matched capability revision, when currently available.
    pub const fn capability_revision(&self) -> Option<u64> {
        self.capability_revision
    }

    /// Returns the exact matched release gate, when currently available.
    pub const fn release_state(&self) -> Option<ProfileReleaseState> {
        self.release_state
    }

    /// Returns the local delegation outcome without credential values.
    pub const fn disposition(&self) -> ProviderCredentialDelegationDisposition {
        self.disposition
    }

    /// Returns the durable local session created for a stored credential, when present.
    pub const fn session_id(&self) -> Option<Uuid> {
        self.session_id
    }

    /// Returns the next bounded onboarding action; credential import never activates a provider.
    pub const fn next_action(&self) -> Option<OnboardingNextAction> {
        self.next_action
    }

    fn disabled(spec: ProviderDelegationSpec) -> Self {
        Self {
            provider: spec.provider,
            selected_surface_id: spec.profile.map(|profile| profile.surface_id),
            capability_revision: None,
            release_state: None,
            disposition: ProviderCredentialDelegationDisposition::Disabled,
            session_id: None,
            next_action: None,
        }
    }

    fn profile_unavailable(
        spec: ProviderDelegationSpec,
        matched: Option<MatchedProfile>,
        reason: ProviderCredentialProfileUnavailableReason,
    ) -> Self {
        Self {
            provider: spec.provider,
            selected_surface_id: spec.profile.map(|profile| profile.surface_id),
            capability_revision: match matched {
                Some(profile) => Some(profile.capability_revision),
                None => None,
            },
            release_state: match matched {
                Some(profile) => Some(profile.release_state),
                None => None,
            },
            disposition: ProviderCredentialDelegationDisposition::ProfileUnavailable(reason),
            session_id: None,
            next_action: None,
        }
    }

    fn probe_required(
        spec: ProviderDelegationSpec,
        matched: MatchedProfile,
        session_id: Uuid,
        next_action: OnboardingNextAction,
    ) -> Self {
        Self {
            provider: spec.provider,
            selected_surface_id: Some(matched.surface_id),
            capability_revision: Some(matched.capability_revision),
            release_state: Some(matched.release_state),
            disposition: ProviderCredentialDelegationDisposition::ProbeRequired,
            session_id: Some(session_id),
            next_action: Some(next_action),
        }
    }

    fn credential_imported(
        spec: ProviderDelegationSpec,
        matched: MatchedProfile,
        session_id: Uuid,
        next_action: OnboardingNextAction,
    ) -> Self {
        Self {
            provider: spec.provider,
            selected_surface_id: Some(matched.surface_id),
            capability_revision: Some(matched.capability_revision),
            release_state: Some(matched.release_state),
            disposition: ProviderCredentialDelegationDisposition::CredentialImported,
            session_id: Some(session_id),
            next_action: Some(next_action),
        }
    }
}

/// Complete secret-free result for the exact V1 provider set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCredentialBundleDelegation {
    providers: Vec<ProviderCredentialDelegationResult>,
}

impl ProviderCredentialBundleDelegation {
    /// Returns the exact schema admitted before delegation.
    pub const fn schema(&self) -> &'static str {
        PROVIDER_CREDENTIAL_BUNDLE_SCHEMA
    }

    /// Returns all providers in the stable V1 contract order.
    pub fn providers(&self) -> &[ProviderCredentialDelegationResult] {
        &self.providers
    }
}

/// Local delegation failure whose formatting never contains credential values.
#[derive(Debug, thiserror::Error)]
pub enum ProviderCredentialBundleDelegationError {
    /// Bounded result storage could not be reserved.
    #[error("provider credential delegation result storage is unavailable")]
    Allocation,
    /// A credential could not be represented in the existing secret-store format.
    #[error("provider credential for {provider:?} could not be encoded")]
    CredentialEncoding {
        /// Code-owned provider identity only.
        provider: ProviderCredentialBundleProvider,
    },
    /// An existing onboarding or secret-store operation rejected the delegation.
    #[error("provider credential delegation failed for {provider:?}")]
    Onboarding {
        /// Code-owned provider identity only.
        provider: ProviderCredentialBundleProvider,
        /// Existing secret-free onboarding failure.
        #[source]
        source: ProviderOnboardingError,
    },
    /// The exact service returned a different surface or lifecycle transition.
    #[error("provider credential delegation invariant failed for {provider:?}")]
    ServiceInvariant {
        /// Code-owned provider identity only.
        provider: ProviderCredentialBundleProvider,
    },
}

/// Delegates one parsed V1 bundle into existing local onboarding and secret-store authorities.
///
/// Every enabled, exactly mapped profile receives a durable local onboarding session. Credentialed
/// profiles store only the admitted write-only secret generation; no-credential profiles stop at
/// an explicit probe-required state. This function never verifies a credential, probes a provider,
/// or activates a runtime.
///
/// # Errors
///
/// Returns [`ProviderCredentialBundleDelegationError`] when local session creation, secret
/// encoding, bounded secret-store mutation, cancellation, or lifecycle validation fails. Earlier
/// provider results can already be durable when a later provider fails; the existing onboarding
/// catalog remains the recovery authority for those sessions.
pub async fn delegate_provider_credential_bundle(
    service: &Arc<ProviderOnboardingService>,
    bundle: ProviderCredentialBundle,
    cancellation: CancellationToken,
) -> Result<ProviderCredentialBundleDelegation, ProviderCredentialBundleDelegationError> {
    let profiles = service.profiles();
    let mut providers = Vec::new();
    providers
        .try_reserve_exact(PROVIDER_COUNT)
        .map_err(|_error| ProviderCredentialBundleDelegationError::Allocation)?;

    for spec in PROVIDER_SPECS {
        if !provider_requested(spec.provider, &bundle.configuration) {
            providers.push(ProviderCredentialDelegationResult::disabled(spec));
            continue;
        }

        match spec.mode {
            ProviderDelegationMode::ProfileUnavailable(reason) => {
                let (matched, reason) = match match_profile(&profiles, spec.profile) {
                    Ok(profile) => (Some(profile), reason),
                    Err(profile_reason) => (None, profile_reason),
                };
                providers.push(ProviderCredentialDelegationResult::profile_unavailable(
                    spec, matched, reason,
                ));
            }
            ProviderDelegationMode::NoSecret => match match_profile(&profiles, spec.profile) {
                Ok(profile) => providers.push(delegate_no_secret(
                    service,
                    spec,
                    profile,
                    &bundle.credentials,
                )?),
                Err(reason) => {
                    providers.push(ProviderCredentialDelegationResult::profile_unavailable(
                        spec, None, reason,
                    ));
                }
            },
            ProviderDelegationMode::Credential(kind) => {
                match match_profile(&profiles, spec.profile) {
                    Ok(profile) => {
                        providers.push(
                            delegate_credential(
                                service,
                                spec,
                                profile,
                                kind,
                                &bundle.credentials,
                                cancellation.child_token(),
                            )
                            .await?,
                        );
                    }
                    Err(reason) => {
                        providers.push(ProviderCredentialDelegationResult::profile_unavailable(
                            spec, None, reason,
                        ));
                    }
                }
            }
        }
    }

    Ok(ProviderCredentialBundleDelegation { providers })
}

async fn delegate_credential(
    service: &Arc<ProviderOnboardingService>,
    spec: ProviderDelegationSpec,
    profile: MatchedProfile,
    kind: CredentialDelegationKind,
    credentials: &ProviderCredentialValues,
    cancellation: CancellationToken,
) -> Result<ProviderCredentialDelegationResult, ProviderCredentialBundleDelegationError> {
    let provider = spec.provider;
    if cancellation.is_cancelled() {
        return Err(onboarding_error(
            provider,
            ProviderOnboardingError::OperationCancelled,
        ));
    }
    let secret = match kind {
        CredentialDelegationKind::SchwabApplication => {
            schwab_application_secret(&credentials.schwab_app_key, &credentials.schwab_app_secret)?
        }
        CredentialDelegationKind::AlpacaPaper => {
            alpaca_paper_secret(&credentials.alpaca_key_id, &credentials.alpaca_secret_key)?
        }
        CredentialDelegationKind::BlsRegistrationKey => {
            copy_secret(provider, &credentials.bls_registration_key)?
        }
        CredentialDelegationKind::BeaUserId => copy_secret(provider, &credentials.bea_user_id)?,
        CredentialDelegationKind::CensusApiKey => {
            copy_secret(provider, &credentials.census_api_key)?
        }
        CredentialDelegationKind::EiaApiKey => copy_secret(provider, &credentials.eia_api_key)?,
        CredentialDelegationKind::FredApiKey => copy_secret(provider, &credentials.fred_api_key)?,
        CredentialDelegationKind::TiingoApiToken => {
            copy_secret(provider, &credentials.tiingo_api_token)?
        }
    };
    let request = StartOnboardingRequest::try_new(profile.surface_id, None, None)
        .map_err(|source| onboarding_error(provider, source))?;
    let started = service
        .start(request, cancellation.child_token())
        .await
        .map_err(|source| onboarding_error(provider, source))?;
    if started.surface_id() != profile.surface_id
        || started.credential_stored()
        || started.next_action() != OnboardingNextAction::ImportSecret
    {
        return Err(ProviderCredentialBundleDelegationError::ServiceInvariant { provider });
    }
    let imported = service
        .submit_secret(started.session_id(), secret, cancellation)
        .await
        .map_err(|source| onboarding_error(provider, source))?;
    if imported.surface_id() != profile.surface_id
        || !imported.credential_stored()
        || imported.next_action() != OnboardingNextAction::VerifyAndActivate
    {
        return Err(ProviderCredentialBundleDelegationError::ServiceInvariant { provider });
    }
    Ok(ProviderCredentialDelegationResult::credential_imported(
        spec,
        profile,
        imported.session_id(),
        imported.next_action(),
    ))
}

fn delegate_no_secret(
    service: &ProviderOnboardingService,
    spec: ProviderDelegationSpec,
    profile: MatchedProfile,
    credentials: &ProviderCredentialValues,
) -> Result<ProviderCredentialDelegationResult, ProviderCredentialBundleDelegationError> {
    let provider = spec.provider;
    let (organization, administrative_email) = if provider == ProviderCredentialBundleProvider::Sec
    {
        (
            Some(copy_public_value(
                provider,
                &credentials.sec_user_agent_organization,
            )?),
            Some(copy_public_value(
                provider,
                &credentials.sec_user_agent_email,
            )?),
        )
    } else {
        (None, None)
    };
    let request =
        StartOnboardingRequest::try_new(profile.surface_id, organization, administrative_email)
            .map_err(|source| onboarding_error(provider, source))?;
    let started = service
        .start_deferred(request)
        .map_err(|source| onboarding_error(provider, source))?;
    if started.surface_id() != profile.surface_id || started.credential_stored() {
        return Err(ProviderCredentialBundleDelegationError::ServiceInvariant { provider });
    }
    Ok(ProviderCredentialDelegationResult::probe_required(
        spec,
        profile,
        started.session_id(),
        started.next_action(),
    ))
}

fn onboarding_error(
    provider: ProviderCredentialBundleProvider,
    source: ProviderOnboardingError,
) -> ProviderCredentialBundleDelegationError {
    ProviderCredentialBundleDelegationError::Onboarding { provider, source }
}

fn alpaca_paper_secret(
    key_id: &ProviderCredentialValue,
    secret_key: &ProviderCredentialValue,
) -> Result<SecretValue, ProviderCredentialBundleDelegationError> {
    alpaca_paper_secret_from_str(key_id.expose_secret(), secret_key.expose_secret())
}

fn schwab_application_secret(
    app_key: &ProviderCredentialValue,
    app_secret: &ProviderCredentialValue,
) -> Result<SecretValue, ProviderCredentialBundleDelegationError> {
    schwab_application_secret_from_str(app_key.expose_secret(), app_secret.expose_secret())
}

fn schwab_application_secret_from_str(
    app_key: &str,
    app_secret: &str,
) -> Result<SecretValue, ProviderCredentialBundleDelegationError> {
    let provider = ProviderCredentialBundleProvider::Schwab;
    let input_bytes = app_key
        .len()
        .checked_add(app_secret.len())
        .and_then(|length| length.checked_mul(2))
        .and_then(|length| length.checked_add(128))
        .ok_or(ProviderCredentialBundleDelegationError::CredentialEncoding { provider })?;
    let mut encoded = Zeroizing::new(Vec::new());
    encoded.try_reserve_exact(input_bytes).map_err(|_error| {
        ProviderCredentialBundleDelegationError::CredentialEncoding { provider }
    })?;
    serde_json::to_writer(
        &mut *encoded,
        &SchwabApplicationCredentialWire {
            version: 1,
            app_key,
            app_secret,
        },
    )
    .map_err(|_error| ProviderCredentialBundleDelegationError::CredentialEncoding { provider })?;
    SecretValue::from_utf8_bytes(std::mem::take(&mut *encoded))
        .map_err(|_error| ProviderCredentialBundleDelegationError::CredentialEncoding { provider })
}

fn alpaca_paper_secret_from_str(
    key_id: &str,
    secret_key: &str,
) -> Result<SecretValue, ProviderCredentialBundleDelegationError> {
    let provider = ProviderCredentialBundleProvider::Alpaca;
    let input_bytes = key_id
        .len()
        .checked_add(secret_key.len())
        .and_then(|length| length.checked_mul(2))
        .and_then(|length| length.checked_add(256))
        .ok_or(ProviderCredentialBundleDelegationError::CredentialEncoding { provider })?;
    let mut encoded = Zeroizing::new(Vec::new());
    encoded.try_reserve_exact(input_bytes).map_err(|_error| {
        ProviderCredentialBundleDelegationError::CredentialEncoding { provider }
    })?;
    serde_json::to_writer(
        &mut *encoded,
        &AlpacaPaperCredentialWire {
            version: 1,
            key_id,
            secret_key,
            trading_api_environment: "paper",
        },
    )
    .map_err(|_error| ProviderCredentialBundleDelegationError::CredentialEncoding { provider })?;
    SecretValue::from_utf8_bytes(std::mem::take(&mut *encoded))
        .map_err(|_error| ProviderCredentialBundleDelegationError::CredentialEncoding { provider })
}

fn copy_secret(
    provider: ProviderCredentialBundleProvider,
    value: &ProviderCredentialValue,
) -> Result<SecretValue, ProviderCredentialBundleDelegationError> {
    let value = value.expose_secret();
    let mut bytes = Zeroizing::new(Vec::new());
    bytes.try_reserve_exact(value.len()).map_err(|_error| {
        ProviderCredentialBundleDelegationError::CredentialEncoding { provider }
    })?;
    bytes.extend_from_slice(value.as_bytes());
    SecretValue::from_utf8_bytes(std::mem::take(&mut *bytes))
        .map_err(|_error| ProviderCredentialBundleDelegationError::CredentialEncoding { provider })
}

fn copy_public_value(
    provider: ProviderCredentialBundleProvider,
    value: &ProviderCredentialValue,
) -> Result<String, ProviderCredentialBundleDelegationError> {
    let value = value.expose_secret();
    let mut copy = String::new();
    copy.try_reserve_exact(value.len()).map_err(|_error| {
        ProviderCredentialBundleDelegationError::CredentialEncoding { provider }
    })?;
    copy.push_str(value);
    Ok(copy)
}

fn provider_requested(
    provider: ProviderCredentialBundleProvider,
    configuration: &ProviderCredentialConfiguration,
) -> bool {
    match provider {
        ProviderCredentialBundleProvider::Schwab => configuration.schwab_enabled,
        ProviderCredentialBundleProvider::Alpaca => configuration.alpaca_enabled,
        ProviderCredentialBundleProvider::YahooFinanceExperimental => {
            configuration.yahoo_finance_experimental_enabled
                || configuration.yahoo_finance_experimental_broad_warm_enabled
        }
        ProviderCredentialBundleProvider::NasdaqTraderReference => {
            configuration.nasdaq_trader_reference_enabled
        }
        ProviderCredentialBundleProvider::OccOptionsReference => {
            configuration.occ_options_reference_enabled
        }
        ProviderCredentialBundleProvider::CboeOptionsReference => {
            configuration.cboe_options_reference_enabled
        }
        ProviderCredentialBundleProvider::IexHist => configuration.iex_hist_enabled,
        ProviderCredentialBundleProvider::Bls => configuration.bls_enabled,
        ProviderCredentialBundleProvider::Bea => configuration.bea_enabled,
        ProviderCredentialBundleProvider::Census => configuration.census_enabled,
        ProviderCredentialBundleProvider::Eia => configuration.eia_enabled,
        ProviderCredentialBundleProvider::FredAlfred => configuration.fred_enabled,
        ProviderCredentialBundleProvider::Tiingo => configuration.tiingo_enabled,
        ProviderCredentialBundleProvider::Sec => configuration.sec_enabled,
        ProviderCredentialBundleProvider::TreasuryFiscalData => {
            configuration.treasury_fiscal_data_enabled
        }
        ProviderCredentialBundleProvider::TreasuryDailyRates => {
            configuration.treasury_daily_rates_enabled
        }
        ProviderCredentialBundleProvider::FederalReserveBoardDirect => {
            configuration.federal_reserve_board_direct_enabled
        }
    }
}

fn match_profile(
    profiles: &[ProviderProfileView],
    expected: Option<RegisteredProfileSpec>,
) -> Result<MatchedProfile, ProviderCredentialProfileUnavailableReason> {
    let expected =
        expected.ok_or(ProviderCredentialProfileUnavailableReason::NoRegisteredSelectedProfile)?;
    let profile = profiles
        .iter()
        .find(|profile| profile.id() == expected.surface_id)
        .ok_or(ProviderCredentialProfileUnavailableReason::NoRegisteredSelectedProfile)?;
    if profile.capability_revision() != expected.capability_revision
        || profile.release_state() != expected.release_state
    {
        return Err(ProviderCredentialProfileUnavailableReason::CapabilityMismatch);
    }
    Ok(MatchedProfile {
        surface_id: expected.surface_id,
        capability_revision: expected.capability_revision,
        release_state: expected.release_state,
    })
}

#[derive(Serialize)]
struct AlpacaPaperCredentialWire<'a> {
    version: u8,
    key_id: &'a str,
    secret_key: &'a str,
    trading_api_environment: &'static str,
}

#[derive(Serialize)]
struct SchwabApplicationCredentialWire<'a> {
    version: u8,
    app_key: &'a str,
    app_secret: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegisteredProfileSpec {
    surface_id: &'static str,
    capability_revision: u64,
    release_state: ProfileReleaseState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MatchedProfile {
    surface_id: &'static str,
    capability_revision: u64,
    release_state: ProfileReleaseState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CredentialDelegationKind {
    SchwabApplication,
    AlpacaPaper,
    BlsRegistrationKey,
    BeaUserId,
    CensusApiKey,
    EiaApiKey,
    FredApiKey,
    TiingoApiToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderDelegationMode {
    ProfileUnavailable(ProviderCredentialProfileUnavailableReason),
    NoSecret,
    Credential(CredentialDelegationKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProviderDelegationSpec {
    provider: ProviderCredentialBundleProvider,
    profile: Option<RegisteredProfileSpec>,
    mode: ProviderDelegationMode,
}

impl ProviderDelegationSpec {
    const fn profile_unavailable(
        provider: ProviderCredentialBundleProvider,
        profile: RegisteredProfileSpec,
        reason: ProviderCredentialProfileUnavailableReason,
    ) -> Self {
        Self {
            provider,
            profile: Some(profile),
            mode: ProviderDelegationMode::ProfileUnavailable(reason),
        }
    }

    const fn no_secret(
        provider: ProviderCredentialBundleProvider,
        profile: RegisteredProfileSpec,
    ) -> Self {
        Self {
            provider,
            profile: Some(profile),
            mode: ProviderDelegationMode::NoSecret,
        }
    }

    const fn credential(
        provider: ProviderCredentialBundleProvider,
        profile: RegisteredProfileSpec,
        kind: CredentialDelegationKind,
    ) -> Self {
        Self {
            provider,
            profile: Some(profile),
            mode: ProviderDelegationMode::Credential(kind),
        }
    }
}

#[cfg(test)]
mod tests {
    use market_squawk_sources::built_in_provider_profiles;

    use super::*;
    use crate::provider_activation::credentials::AlpacaCredentialEnvelope;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn selected_profile_mapping_and_omissions_are_exact() -> TestResult {
        let profiles = built_in_provider_profiles()?;
        assert_eq!(PROVIDER_SPECS.len(), PROVIDER_COUNT);
        assert_eq!(
            PROVIDER_SPECS.map(|spec| spec.provider),
            [
                ProviderCredentialBundleProvider::Schwab,
                ProviderCredentialBundleProvider::Alpaca,
                ProviderCredentialBundleProvider::YahooFinanceExperimental,
                ProviderCredentialBundleProvider::NasdaqTraderReference,
                ProviderCredentialBundleProvider::OccOptionsReference,
                ProviderCredentialBundleProvider::CboeOptionsReference,
                ProviderCredentialBundleProvider::IexHist,
                ProviderCredentialBundleProvider::Bls,
                ProviderCredentialBundleProvider::Bea,
                ProviderCredentialBundleProvider::Census,
                ProviderCredentialBundleProvider::Eia,
                ProviderCredentialBundleProvider::FredAlfred,
                ProviderCredentialBundleProvider::Tiingo,
                ProviderCredentialBundleProvider::Sec,
                ProviderCredentialBundleProvider::TreasuryFiscalData,
                ProviderCredentialBundleProvider::TreasuryDailyRates,
                ProviderCredentialBundleProvider::FederalReserveBoardDirect,
            ]
        );

        for spec in PROVIDER_SPECS {
            let expected = spec.profile.ok_or("selected provider mapping is missing")?;
            let profile = profiles
                .get(expected.surface_id)
                .ok_or("selected provider profile is missing")?;
            assert_eq!(
                profile.capability().revision().get(),
                expected.capability_revision
            );
            assert_eq!(profile.release_state(), expected.release_state);
        }
        assert_eq!(
            PROVIDER_SPECS[7].mode,
            ProviderDelegationMode::Credential(CredentialDelegationKind::BlsRegistrationKey)
        );
        Ok(())
    }

    #[test]
    fn alpaca_envelope_is_exactly_paper_and_redacted() -> TestResult {
        let secret = alpaca_paper_secret_from_str("fixture-key-id", "fixture-secret-key")?;
        assert_eq!(
            secret.expose_secret(),
            r#"{"version":1,"key_id":"fixture-key-id","secret_key":"fixture-secret-key","trading_api_environment":"paper"}"#
        );
        let envelope = AlpacaCredentialEnvelope::try_parse(secret.expose_secret())?;
        assert_eq!(
            envelope.trading_api_environment(),
            market_squawk_adapter_alpaca::AlpacaTradingApiEnvironment::Paper
        );
        assert_eq!(format!("{secret:?}"), "SecretValue([REDACTED])");
        assert!(!format!("{secret:?}").contains("fixture-secret-key"));
        Ok(())
    }

    #[test]
    fn schwab_envelope_is_exact_and_redacted() -> TestResult {
        let secret = schwab_application_secret_from_str("fixture-app-key", "fixture-app-secret")?;
        assert_eq!(
            secret.expose_secret(),
            r#"{"version":1,"app_key":"fixture-app-key","app_secret":"fixture-app-secret"}"#
        );
        let envelope =
            market_squawk_adapter_schwab::SchwabApplicationCredentialEnvelope::try_parse(
                secret.expose_secret(),
            )?;
        assert_eq!(envelope.expose_app_key(), "fixture-app-key");
        assert_eq!(
            format!("{envelope:?}"),
            "SchwabApplicationCredentialEnvelope([REDACTED])"
        );
        assert!(!format!("{secret:?}").contains("fixture-app-secret"));
        Ok(())
    }

    #[test]
    fn requested_mapping_keeps_disabled_and_yahoo_modes_explicit() {
        let mut configuration = ProviderCredentialConfiguration {
            schwab_enabled: false,
            alpaca_enabled: false,
            alpaca_trading_api_environment:
                super::super::credential_bundle::AlpacaCredentialRealm::Paper,
            yahoo_finance_experimental_enabled: false,
            yahoo_finance_experimental_broad_warm_enabled: false,
            nasdaq_trader_reference_enabled: false,
            occ_options_reference_enabled: false,
            cboe_options_reference_enabled: false,
            iex_hist_enabled: false,
            bls_enabled: false,
            bea_enabled: false,
            census_enabled: false,
            eia_enabled: false,
            fred_enabled: false,
            tiingo_enabled: false,
            sec_enabled: false,
            treasury_fiscal_data_enabled: false,
            treasury_daily_rates_enabled: false,
            federal_reserve_board_direct_enabled: false,
        };
        for spec in PROVIDER_SPECS {
            assert!(!provider_requested(spec.provider, &configuration));
        }

        configuration.yahoo_finance_experimental_broad_warm_enabled = true;
        assert!(provider_requested(
            ProviderCredentialBundleProvider::YahooFinanceExperimental,
            &configuration
        ));
        assert!(!provider_requested(
            ProviderCredentialBundleProvider::Alpaca,
            &configuration
        ));
    }
}
