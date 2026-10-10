//! Shared native coverage choices. Financial components retain their original authorities.

use super::{
    AnalyticalControllerResponse, AnalyticalProfile, AnalyticalProfileConfig,
    AnalyticalProfileKind, AnalyticalWorkflowController, ProfileHistoryAction,
    ProfileValidationState, WorkflowError, opaque_profile_state_token, profile_conflict,
    profile_index_from_token, profile_presentation, valid_display_name,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisScope {
    Focused,
    Balanced,
    Broad,
}

impl AnalysisScope {
    /// Limits expensive per-investment work after complete supported-universe enumeration.
    pub(super) const fn maximum_analyses(self) -> usize {
        match self {
            Self::Focused => 8,
            Self::Balanced => 16,
            Self::Broad => 32,
        }
    }

    pub(super) fn from_config(config: &AnalyticalProfileConfig) -> Self {
        config.discovery_breadth
    }
}

impl AnalyticalWorkflowController {
    pub(super) fn update_custom(
        &self,
        profile_token: &str,
        profile_state_token: &str,
        display_name: String,
        scope: AnalysisScope,
        preferences: super::financial_profiles::FinancialPreferencesInput,
    ) -> Result<AnalyticalControllerResponse, WorkflowError> {
        if !valid_display_name(&display_name) || display_name == super::DEFAULT_PROFILE_NAME {
            return Err(WorkflowError::invalid_request(
                "Choose a distinct profile name between 1 and 64 visible characters.",
            ));
        }
        self.mutate(|document, now| {
            let index = profile_index_from_token(document, profile_token)?;
            let current = document.profiles[index].clone();
            if current.kind != AnalyticalProfileKind::Custom
                || document.active_profile.profile_id == current.profile_id
                || opaque_profile_state_token(&current)? != profile_state_token
            {
                return Err(profile_conflict());
            }
            if document.profiles.iter().any(|other| {
                other.profile_id != current.profile_id && other.display_name == display_name
            }) {
                return Err(WorkflowError::invalid_request(
                    "Another analysis profile already uses that name.",
                ));
            }
            let mut config = current.config.clone();
            config.discovery_breadth = scope;
            config.financial_configuration = config
                .financial_configuration
                .with_preferences(preferences)?;
            if config == current.config && display_name == current.display_name {
                return Ok(AnalyticalControllerResponse::Profile {
                    profile: profile_presentation(document, &current)?,
                });
            }
            let updated = AnalyticalProfile {
                display_name,
                revision: current
                    .revision
                    .checked_add(1)
                    .ok_or_else(WorkflowError::internal)?,
                config_digest: config.digest()?,
                config,
                validation_state: ProfileValidationState::NotValidated,
                last_validation: None,
                updated_at: now.clone(),
                ..current
            };
            document.revision = document.next_revision()?;
            document.profiles[index] = updated.clone();
            document.append_history(
                ProfileHistoryAction::UpdatedCustom,
                &updated,
                Some(updated.profile_id),
                now,
            )?;
            Ok(AnalyticalControllerResponse::Profile {
                profile: profile_presentation(document, &updated)?,
            })
        })
    }
}
