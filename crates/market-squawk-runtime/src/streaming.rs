//! Validated native-input tickets and generation-aware event cursors.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ClientId, InputTicketId, InstallationId, RuntimeContractError, RuntimeIdentity,
    ServiceGeneration, WorkspaceId,
};

const MAXIMUM_EVENT_PAGE_ITEMS: usize = 4_096;

/// Immutable metadata for one native-streamed input staged by the service.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputTicket {
    id: InputTicketId,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    generation: ServiceGeneration,
    client_id: ClientId,
    media_type: SourceIdentifier,
    byte_length: u64,
    digest: EvidenceDigest,
    expires_at: Timestamp,
}

impl InputTicket {
    /// Creates an opaque expiring reference after native streaming and digest verification.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        id: InputTicketId,
        installation_id: InstallationId,
        workspace_id: WorkspaceId,
        generation: ServiceGeneration,
        client_id: ClientId,
        media_type: SourceIdentifier,
        byte_length: u64,
        digest: EvidenceDigest,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<Self, RuntimeContractError> {
        if byte_length == 0 {
            return Err(RuntimeContractError::EmptyInput);
        }
        if expires_at <= now {
            return Err(RuntimeContractError::ExpiredDeadline);
        }
        Ok(Self {
            id,
            installation_id,
            workspace_id,
            generation,
            client_id,
            media_type,
            byte_length,
            digest,
            expires_at,
        })
    }

    /// Decodes a ticket and revalidates its exact runtime, client, input evidence, and lifetime.
    pub fn decode_expected(
        encoded: &[u8],
        runtime: RuntimeIdentity,
        client_id: ClientId,
        admission: &InputAdmission,
        now: Timestamp,
        maximum_encoded_bytes: usize,
    ) -> Result<Self, RuntimeContractError> {
        if encoded.len() > maximum_encoded_bytes {
            return Err(RuntimeContractError::InvalidResponse);
        }
        let wire: InputTicketWire =
            serde_json::from_slice(encoded).map_err(|_| RuntimeContractError::InvalidResponse)?;
        let ticket = Self::try_new(
            wire.id,
            wire.installation_id,
            wire.workspace_id,
            wire.generation,
            wire.client_id,
            wire.media_type,
            wire.byte_length,
            wire.digest,
            wire.expires_at,
            now,
        )?;
        if ticket.installation_id != runtime.installation_id()
            || ticket.workspace_id != runtime.workspace_id()
            || ticket.generation != runtime.service_generation()
            || ticket.client_id != client_id
            || ticket.byte_length != admission.expected_bytes()
            || ticket.digest != admission.expected_digest()
            || ticket.media_type != *admission.media_type()
        {
            return Err(RuntimeContractError::IdentityMismatch);
        }
        Ok(ticket)
    }

    /// Returns the opaque input identity.
    #[must_use]
    pub const fn id(&self) -> InputTicketId {
        self.id
    }

    /// Exact installation that owns the staged bytes.
    #[must_use]
    pub const fn installation_id(&self) -> InstallationId {
        self.installation_id
    }

    /// Exact workspace that owns the staged bytes.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Service generation that created this ticket.
    #[must_use]
    pub const fn generation(&self) -> ServiceGeneration {
        self.generation
    }

    /// Registered client that owns this ticket.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Media type verified during staging.
    #[must_use]
    pub const fn media_type(&self) -> &SourceIdentifier {
        &self.media_type
    }

    /// Expiration after which the ticket cannot be resolved.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Verified staged byte length.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    /// Verified digest of the staged bytes.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InputTicketWire {
    id: InputTicketId,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    generation: ServiceGeneration,
    client_id: ClientId,
    media_type: SourceIdentifier,
    byte_length: u64,
    digest: EvidenceDigest,
    expires_at: Timestamp,
}

/// Reconnect cursor for one client's bounded service-generation event projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventCursor {
    client_id: ClientId,
    generation: ServiceGeneration,
    sequence: u64,
    expires_at: Timestamp,
}

impl EventCursor {
    /// Creates an expiring cursor. Sequence zero means no event observed yet.
    pub fn try_new(
        client_id: ClientId,
        generation: ServiceGeneration,
        sequence: u64,
        expires_at: Timestamp,
    ) -> Result<Self, RuntimeContractError> {
        if expires_at.unix_nanos() <= 0 {
            return Err(RuntimeContractError::ExpiredDeadline);
        }
        Ok(Self {
            client_id,
            generation,
            sequence,
            expires_at,
        })
    }

    /// Ensures this cursor belongs to the authenticated client and current generation.
    pub fn ensure_current(
        &self,
        client_id: ClientId,
        generation: ServiceGeneration,
        now: Timestamp,
    ) -> Result<(), EventCursorError> {
        if self.client_id != client_id {
            Err(EventCursorError::ClientChanged)
        } else if self.generation != generation {
            Err(EventCursorError::GenerationChanged)
        } else if self.expires_at <= now {
            Err(EventCursorError::Expired)
        } else {
            Ok(())
        }
    }

    /// Registered client that owns this cursor.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Last observed sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Service generation that owns this cursor.
    #[must_use]
    pub const fn generation(&self) -> ServiceGeneration {
        self.generation
    }

    /// Cursor retention deadline.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Event cursor cannot continue without a fresh snapshot.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EventCursorError {
    /// The cursor belongs to another registered client.
    #[error("event cursor client changed; snapshot resynchronization is required")]
    ClientChanged,
    /// The service or workspace switched generations.
    #[error("event cursor generation changed; snapshot resynchronization is required")]
    GenerationChanged,
    /// The bounded cursor retention window elapsed.
    #[error("event cursor expired; snapshot resynchronization is required")]
    Expired,
}

/// Maximum records returned by one application-event page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventPageLimit(usize);

impl EventPageLimit {
    /// Creates a positive limit no larger than 4,096 events.
    pub fn try_new(value: usize) -> Result<Self, RuntimeContractError> {
        if value == 0 || value > MAXIMUM_EVENT_PAGE_ITEMS {
            Err(RuntimeContractError::InvalidPageLimit)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the admitted item count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Request metadata for native-streamed input staging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputAdmission {
    media_type: SourceIdentifier,
    expected_bytes: u64,
    expected_digest: EvidenceDigest,
}

impl InputAdmission {
    /// Admits a nonempty byte stream with exact content evidence.
    pub fn try_new(
        media_type: SourceIdentifier,
        expected_bytes: u64,
        expected_digest: EvidenceDigest,
    ) -> Result<Self, RuntimeContractError> {
        if expected_bytes == 0 {
            Err(RuntimeContractError::EmptyInput)
        } else {
            Ok(Self {
                media_type,
                expected_bytes,
                expected_digest,
            })
        }
    }

    /// Admits a nonempty SHA-256-addressed byte stream across the runtime boundary.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeContractError::InvalidPayload`] when `media_type` is invalid and
    /// [`RuntimeContractError::EmptyInput`] when `expected_bytes` is zero.
    pub fn try_sha256(
        media_type: &str,
        expected_bytes: u64,
        expected_digest: [u8; 32],
    ) -> Result<Self, RuntimeContractError> {
        let media_type = SourceIdentifier::try_from(media_type)
            .map_err(|_error| RuntimeContractError::InvalidPayload)?;
        Self::try_new(
            media_type,
            expected_bytes,
            EvidenceDigest::new(DigestAlgorithm::Sha256, expected_digest),
        )
    }

    /// Declared media type of the staged bytes.
    #[must_use]
    pub const fn media_type(&self) -> &SourceIdentifier {
        &self.media_type
    }

    /// Exact byte length required from the stream.
    #[must_use]
    pub const fn expected_bytes(&self) -> u64 {
        self.expected_bytes
    }

    /// Exact digest required from the stream.
    #[must_use]
    pub const fn expected_digest(&self) -> EvidenceDigest {
        self.expected_digest
    }
}
