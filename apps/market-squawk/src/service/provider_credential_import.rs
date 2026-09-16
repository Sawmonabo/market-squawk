//! Secret-safe installed import of the owner-managed provider credential bundle.

use std::{fmt, sync::Arc, time::Instant};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_runtime::{ClientId, InputStager, InputTicketId, RuntimeIdentity};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::provider_onboarding::{
    PROVIDER_CREDENTIAL_BUNDLE_SCHEMA, ProviderCredentialBundleDelegationError,
    ProviderCredentialDelegationDisposition, ProviderCredentialProfileUnavailableReason,
    ProviderOnboardingService, delegate_provider_credential_bundle,
};

pub(super) const IMPORT_PROVIDER_CREDENTIAL_BUNDLE: &str = "Source.ImportCredentialBundle";
pub(super) const PROVIDER_CREDENTIAL_BUNDLE_MEDIA_TYPE: &str =
    "market-squawk.provider-credentials.v1";
const MAXIMUM_PROVIDER_CREDENTIAL_BUNDLE_BYTES: u64 = 64 * 1024;

/// Installed adapter that claims one native-staged credential file and returns no values.
#[derive(Clone)]
pub(super) struct InstalledProviderCredentialImport {
    onboarding: Arc<ProviderOnboardingService>,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    activation: Arc<dyn crate::ProviderPortalActivationAuthority>,
    desktop_client: ClientId,
    cli_client: ClientId,
    import_gate: Arc<tokio::sync::Mutex<()>>,
}

impl InstalledProviderCredentialImport {
    pub(super) fn new(
        onboarding: Arc<ProviderOnboardingService>,
        inputs: Arc<InputStager>,
        runtime: RuntimeIdentity,
        activation: Arc<dyn crate::ProviderPortalActivationAuthority>,
        desktop_client: ClientId,
        cli_client: ClientId,
    ) -> Self {
        Self {
            onboarding,
            inputs,
            runtime,
            activation,
            desktop_client,
            cli_client,
            import_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(super) fn session_gate(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.import_gate)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.authorize(context)?;
        ensure_live(context)?;
        let _guard = tokio::select! {
            biased;
            () = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(context.deadline().into()) => return Err(ServiceError::DeadlineExceeded),
            guard = self.import_gate.lock() => guard,
        };
        let input: ImportRequest = serde_json::from_value(Value::Object(
            super::business_arguments(request.arguments()),
        ))
        .map_err(|_error| ServiceError::InvalidRequest)?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let client = ClientId::try_from_uuid(origin.client_id())
            .map_err(|_error| ServiceError::Unauthorized)?;
        let ticket_id = InputTicketId::try_from_uuid(input.input_ticket_id)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let media_type = SourceIdentifier::try_from(PROVIDER_CREDENTIAL_BUNDLE_MEDIA_TYPE)
            .map_err(|_error| ServiceError::Unavailable)?;
        let now = current_timestamp()?;
        let claimed = self
            .inputs
            .claim(ticket_id, client, &media_type, now)
            .map_err(|_error| ServiceError::Unauthorized)?;
        let bytes = claimed
            .read_verified(MAXIMUM_PROVIDER_CREDENTIAL_BUNDLE_BYTES)
            .map(Zeroizing::new)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let bundle =
            crate::provider_onboarding::credential_bundle::parse_provider_credential_bundle_bytes(
                &bytes,
            )
            .map_err(|_error| ServiceError::InvalidRequest)?;
        drop(bytes);
        let result = delegate_provider_credential_bundle(
            &self.onboarding,
            self.activation.as_ref(),
            bundle,
            context.cancellation().child_token(),
        )
        .await
        .map_err(map_delegation_error)?;
        ensure_live(context)?;

        let providers = result
            .providers()
            .iter()
            .map(|provider| {
                let (enabled, disposition) = match provider.disposition() {
                    ProviderCredentialDelegationDisposition::Disabled => (false, "disabled"),
                    ProviderCredentialDelegationDisposition::ProbeRequired => {
                        (true, "probe_required")
                    }
                    ProviderCredentialDelegationDisposition::SavedSetupReused => {
                        (true, "saved_setup_reused")
                    }
                    ProviderCredentialDelegationDisposition::CredentialImported => {
                        (true, "credential_stored_unverified")
                    }
                    ProviderCredentialDelegationDisposition::ProfileUnavailable(
                        ProviderCredentialProfileUnavailableReason::NoRegisteredSelectedProfile
                        | ProviderCredentialProfileUnavailableReason::CapabilityMismatch
                        | ProviderCredentialProfileUnavailableReason::ExactBundleCannotSatisfyProfile,
                    ) => (true, "profile_unavailable"),
                };
                json!({
                    "provider": provider.provider().as_str(),
                    "enabled": enabled,
                    "disposition": disposition,
                    "onboardingSessionId": provider.session_id(),
                })
            })
            .collect::<Vec<_>>();
        let value = json!({
            "schema": PROVIDER_CREDENTIAL_BUNDLE_SCHEMA,
            "providers": providers,
        });
        TypedToolResult::try_new(
            value,
            result.providers().len(),
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid()
            || (origin.client_id() != self.desktop_client.as_uuid()
                && origin.client_id() != self.cli_client.as_uuid())
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }
}

impl fmt::Debug for InstalledProviderCredentialImport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledProviderCredentialImport")
            .field("onboarding", &"[PROTECTED PROVIDER ONBOARDING AUTHORITY]")
            .field("inputs", &"[ONE-SHOT INPUT STAGER]")
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImportRequest {
    input_ticket_id: Uuid,
}

fn map_delegation_error(error: ProviderCredentialBundleDelegationError) -> ServiceError {
    // Transport deliberately collapses authority failures. Retain only code-owned labels here
    // so a failed import can be diagnosed without logging the bundle, paths, references or keys.
    match error {
        ProviderCredentialBundleDelegationError::Allocation => {
            tracing::warn!(failure = "allocation", "provider credential import failed");
            ServiceError::ResourceExhausted
        }
        ProviderCredentialBundleDelegationError::CredentialEncoding { provider } => {
            tracing::warn!(
                provider = provider.as_str(),
                failure = "credential_encoding",
                "provider credential import failed"
            );
            ServiceError::InvalidRequest
        }
        ProviderCredentialBundleDelegationError::ServiceInvariant { provider } => {
            tracing::warn!(
                provider = provider.as_str(),
                failure = "service_invariant",
                "provider credential import failed"
            );
            ServiceError::InvalidRequest
        }
        ProviderCredentialBundleDelegationError::SavedStateUnavailable => {
            tracing::warn!(failure = "saved_state", "provider credential import failed");
            ServiceError::Unavailable
        }
        ProviderCredentialBundleDelegationError::Onboarding { provider, source } => {
            use crate::provider_onboarding::ProviderOnboardingError;
            if let ProviderOnboardingError::SecretStore(error) = &source {
                // LocalSecretStoreError is a closed enum with fixed, value-free messages.
                tracing::warn!(provider = provider.as_str(), failure = "secret_store", reason = %error, "provider credential import failed");
            } else {
                let failure = match source {
                    ProviderOnboardingError::Catalog(_) => "catalog",
                    ProviderOnboardingError::InvalidSessionState => "invalid_session_state",
                    ProviderOnboardingError::SecretImportUnavailable => "secret_import_unavailable",
                    ProviderOnboardingError::InvalidSecretShape => "invalid_secret_shape",
                    ProviderOnboardingError::EvidenceRefreshRequired => "evidence_refresh_required",
                    ProviderOnboardingError::OperationCancelled => "cancelled",
                    ProviderOnboardingError::SecretOperationUnavailable => {
                        "secret_operation_unavailable"
                    }
                    ProviderOnboardingError::Clock => "clock",
                    _ => "onboarding_contract",
                };
                tracing::warn!(
                    provider = provider.as_str(),
                    failure,
                    "provider credential import failed"
                );
            }
            ServiceError::Unavailable
        }
    }
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
    super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
