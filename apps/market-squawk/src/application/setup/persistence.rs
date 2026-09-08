use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_runtime::WorkspaceId;
use serde::{Deserialize, Serialize};

use super::{AcceptedSetupPlan, SETUP_PLAN_FORMAT_VERSION, SetupPlanError};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct SetupPlanDocument {
    format_version: u16,
    owner_workspace: WorkspaceId,
    accepted_plan: AcceptedSetupPlan,
}

impl SetupPlanDocument {
    pub(super) fn try_new(
        owner_workspace: WorkspaceId,
        accepted_plan: AcceptedSetupPlan,
    ) -> Result<Self, SetupPlanError> {
        let document = Self {
            format_version: SETUP_PLAN_FORMAT_VERSION,
            owner_workspace,
            accepted_plan,
        };
        document.validate(owner_workspace)?;
        Ok(document)
    }

    pub(super) fn decode(
        encoded: &[u8],
        expected_owner: WorkspaceId,
    ) -> Result<Self, SetupPlanError> {
        serde_json::from_slice::<Self>(encoded)
            .map_err(|_| SetupPlanError::CorruptState)?
            .validated(expected_owner)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, SetupPlanError> {
        self.validate(self.owner_workspace)?;
        let encoded = serde_json::to_vec(self).map_err(|_| SetupPlanError::Encoding)?;
        if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
            return Err(SetupPlanError::CapacityExceeded);
        }
        Ok(encoded)
    }

    pub(super) const fn revision(&self) -> u64 {
        self.accepted_plan.revision()
    }

    pub(super) const fn accepted_plan(&self) -> &AcceptedSetupPlan {
        &self.accepted_plan
    }

    fn validated(self, expected_owner: WorkspaceId) -> Result<Self, SetupPlanError> {
        self.validate(expected_owner)?;
        Ok(self)
    }

    fn validate(&self, expected_owner: WorkspaceId) -> Result<(), SetupPlanError> {
        if self.format_version != SETUP_PLAN_FORMAT_VERSION
            || self.owner_workspace != expected_owner
        {
            return Err(SetupPlanError::CorruptState);
        }
        self.accepted_plan.validate(expected_owner)
    }
}
