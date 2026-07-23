//! Bounded, process-local discovery selections for exact research ingestion.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, MAX_DISCOVERY_OBJECTS, SourceMetadata,
    SourceObject,
};
use serde::Serialize;
use uuid::Uuid;

use super::{
    CoordinatorAuthority, ManagedResearchExtractionSource, ResearchRightsAuthority,
    ResearchSourceDiscoveryRights,
};

/// One exact discovered object paired with its opaque, single-use ingestion authority.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSourceDiscoveryObject {
    #[serde(flatten)]
    source_object: SourceObject,
    #[serde(skip)]
    receipt: Uuid,
    discovery_receipt: String,
    discovery_receipt_expires_at: Timestamp,
}

impl ResearchSourceDiscoveryObject {
    /// Returns the complete source object retained for receipt-mediated ingestion.
    pub const fn source_object(&self) -> &SourceObject {
        &self.source_object
    }

    /// Returns the opaque, server-minted single-use selection receipt.
    pub fn discovery_receipt(&self) -> &str {
        &self.discovery_receipt
    }

    /// Returns the wall-clock expiry advertised for this receipt.
    pub const fn discovery_receipt_expires_at(&self) -> Timestamp {
        self.discovery_receipt_expires_at
    }
}

/// Bounded, authority-preserving producer contract for one registered research source.
///
/// Each object has a distinct process-local receipt. Receipts are deliberately invalid after a
/// process restart and bind the exact profile, metadata, rights, request, and complete source
/// object retained by the coordinator.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSourceDiscovery {
    profile: SourceIdentifier,
    metadata: SourceMetadata,
    rights: ResearchSourceDiscoveryRights,
    request: DiscoveryRequest,
    objects: Vec<ResearchSourceDiscoveryObject>,
    receipts_survive_restart: bool,
}

impl ResearchSourceDiscovery {
    /// Returns the active provider profile that owns these objects.
    pub const fn profile(&self) -> &SourceIdentifier {
        &self.profile
    }

    /// Returns exact registered metadata, including coverage and quality ceilings.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns retained persistence-rights evidence.
    pub const fn rights(&self) -> &ResearchSourceDiscoveryRights {
        &self.rights
    }

    /// Returns the exact bounded request used by the adapter.
    pub const fn request(&self) -> &DiscoveryRequest {
        &self.request
    }

    /// Returns request-bound exact objects and their single-use ingestion receipts.
    pub fn objects(&self) -> &[ResearchSourceDiscoveryObject] {
        &self.objects
    }

    /// Returns whether these process-local receipts remain valid after a restart.
    pub const fn receipts_survive_restart(&self) -> bool {
        self.receipts_survive_restart
    }
}

pub(super) struct RetainedDiscoverySelections {
    entries: Vec<RetainedDiscoverySelection>,
}

impl RetainedDiscoverySelections {
    pub(super) const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact receipt bindings and paired expiry clocks remain explicit"
    )]
    pub(super) fn mint(
        &mut self,
        profile: &SourceIdentifier,
        metadata: &SourceMetadata,
        rights: &ResearchRightsAuthority,
        discovery: DiscoveryBatch,
        retention: Duration,
        observed_monotonic: Instant,
        observed_wall: Timestamp,
        operation_deadline: Instant,
    ) -> Result<ResearchSourceDiscovery, ServiceError> {
        self.prune_expired(observed_monotonic, observed_wall);
        let object_count = discovery.objects().len();
        let retained_count = self
            .entries
            .len()
            .checked_add(object_count)
            .ok_or(ServiceError::ResourceExhausted)?;
        if retained_count > MAX_DISCOVERY_OBJECTS {
            return Err(ServiceError::ResourceExhausted);
        }

        let discovery_rights = rights.discovery_evidence(observed_wall)?;
        let retention_nanos =
            i64::try_from(retention.as_nanos()).map_err(|_error| ServiceError::Internal)?;
        let retention_wall_expiry = observed_wall
            .checked_add_nanos(retention_nanos)
            .map_err(|_error| ServiceError::Internal)?;
        let receipt_expiry = rights
            .authorization_expires_at
            .map_or(retention_wall_expiry, |rights_expiry| {
                rights_expiry.min(retention_wall_expiry)
            });
        if receipt_expiry <= observed_wall {
            return Err(ServiceError::Unauthorized);
        }
        let monotonic_retention = match rights.authorization_expires_at {
            Some(rights_expiry) => {
                let rights_nanos = rights_expiry
                    .unix_nanos()
                    .checked_sub(observed_wall.unix_nanos())
                    .ok_or(ServiceError::Unauthorized)?;
                let rights_nanos =
                    u64::try_from(rights_nanos).map_err(|_error| ServiceError::Unauthorized)?;
                retention.min(Duration::from_nanos(rights_nanos))
            }
            None => retention,
        };
        let monotonic_expiry = observed_monotonic
            .checked_add(monotonic_retention)
            .ok_or(ServiceError::Internal)?;

        let mut pending = Vec::new();
        pending
            .try_reserve_exact(object_count)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(object_count)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for object in discovery.objects() {
            let receipt = Uuid::new_v4();
            if self
                .entries
                .iter()
                .chain(pending.iter())
                .any(|selection| selection.receipt == receipt)
            {
                return Err(ServiceError::Internal);
            }
            let receipt_text = receipt.hyphenated().to_string();
            pending.push(RetainedDiscoverySelection {
                receipt,
                profile: profile.clone(),
                metadata: metadata.clone(),
                rights: rights.clone(),
                request: discovery.request().clone(),
                object: object.clone(),
                monotonic_expiry,
                wall_expiry: receipt_expiry,
            });
            objects.push(ResearchSourceDiscoveryObject {
                source_object: object.clone(),
                receipt,
                discovery_receipt: receipt_text,
                discovery_receipt_expires_at: receipt_expiry,
            });
        }

        self.entries
            .try_reserve_exact(object_count)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        if Instant::now() >= operation_deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        self.entries.extend(pending);
        Ok(ResearchSourceDiscovery {
            profile: profile.clone(),
            metadata: metadata.clone(),
            rights: discovery_rights,
            request: discovery.request().clone(),
            objects,
            receipts_survive_restart: false,
        })
    }

    fn prune_expired(&mut self, observed_monotonic: Instant, observed_wall: Timestamp) {
        self.entries.retain(|selection| {
            selection.monotonic_expiry > observed_monotonic && selection.wall_expiry > observed_wall
        });
    }

    pub(super) fn revoke(
        &mut self,
        discovery: &ResearchSourceDiscovery,
    ) -> Result<(), ServiceError> {
        if discovery.objects.iter().enumerate().any(|(index, object)| {
            discovery.objects[index.saturating_add(1)..]
                .iter()
                .any(|candidate| candidate.receipt == object.receipt)
        }) {
            return Err(ServiceError::InvalidResult);
        }
        for selection in &self.entries {
            let Some(object) = discovery
                .objects
                .iter()
                .find(|object| object.receipt == selection.receipt)
            else {
                continue;
            };
            if selection.profile != discovery.profile
                || selection.metadata != discovery.metadata
                || selection.request != discovery.request
                || selection.object != object.source_object
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        self.entries.retain(|selection| {
            !discovery
                .objects
                .iter()
                .any(|object| object.receipt == selection.receipt)
        });
        Ok(())
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }
}

struct RetainedDiscoverySelection {
    receipt: Uuid,
    profile: SourceIdentifier,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
    request: DiscoveryRequest,
    object: SourceObject,
    monotonic_expiry: Instant,
    wall_expiry: Timestamp,
}

pub(super) struct PreparedRetainedSelection {
    pub(super) source: Arc<dyn ManagedResearchExtractionSource>,
    pub(super) metadata: SourceMetadata,
    pub(super) rights: ResearchRightsAuthority,
    pub(super) authority: ExtractionAuthority,
    pub(super) object: SourceObject,
}

impl CoordinatorAuthority {
    pub(super) fn consume_discovery_selection(
        &mut self,
        receipt_text: &str,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        object_id: &SourceIdentifier,
        observed_monotonic: Instant,
        observed_wall: Timestamp,
    ) -> Result<PreparedRetainedSelection, ServiceError> {
        let receipt = parse_canonical_receipt(receipt_text)?;
        self.selections
            .prune_expired(observed_monotonic, observed_wall);
        let selection_index = self
            .selections
            .entries
            .iter()
            .position(|selection| selection.receipt == receipt)
            .ok_or(ServiceError::NotFound)?;
        let selection = &self.selections.entries[selection_index];
        if &selection.profile != profile
            || selection.request.dataset() != dataset
            || selection.object.dataset() != dataset
            || selection.object.object_id() != object_id
        {
            return Err(ServiceError::InvalidRequest);
        }
        if selection.object.discovery_request_id() != selection.request.request_id()
            || selection.object.source_id() != selection.metadata.source_id()
            || selection.object.metadata_revision() != selection.metadata.revision()
        {
            return Err(ServiceError::InvalidResult);
        }

        let registry = self.registry.as_ref().ok_or(ServiceError::Unavailable)?;
        let registered = self.sources.get(profile).ok_or(ServiceError::NotFound)?;
        if registered.metadata != selection.metadata
            || registered.rights != selection.rights
            || registered.source.metadata() != &selection.metadata
        {
            return Err(ServiceError::NotFound);
        }
        selection.rights.validate_at(observed_wall)?;
        let authority = registry
            .extraction_authority(&registered.registration, registered.source.as_ref())
            .map_err(|_error| ServiceError::Unavailable)?;
        if authority.metadata() != &selection.metadata {
            return Err(ServiceError::NotFound);
        }
        let source = Arc::clone(&registered.source);
        let selection = self.selections.entries.swap_remove(selection_index);
        Ok(PreparedRetainedSelection {
            source,
            metadata: selection.metadata,
            rights: selection.rights,
            authority,
            object: selection.object,
        })
    }
}

fn parse_canonical_receipt(value: &str) -> Result<Uuid, ServiceError> {
    let receipt = Uuid::parse_str(value).map_err(|_error| ServiceError::InvalidRequest)?;
    if receipt.hyphenated().to_string() != value {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(receipt)
}
