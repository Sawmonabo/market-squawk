//! Exact presentation authority for the isolated installed Alpaca fixture.

use std::sync::Arc;

use market_squawk_domain::{
    ConnectionGeneration, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};
use market_squawk_services::ServiceError;

use crate::{
    alpaca_installed_fixture::{
        InstalledFixtureInstrumentDefinition, InstalledFixtureInstrumentRouteId,
    },
    live_source::display_market::{
        DisplayMarketAvailability, DisplayMarketKey, DisplayMarketPayload,
        DisplayMarketSnapshotLease,
        runtime::{
            InstalledFixtureDisplayReadAuthority, InstalledFixtureDisplayRuntimeError,
            InstalledFixtureDisplaySourceRuntime,
        },
    },
};

/// Immutable, noncanonical presentation descriptor for one exact fixture runtime generation.
#[derive(Debug)]
pub(crate) struct InstalledFixtureDisplayDescriptor {
    definition: Arc<InstalledFixtureInstrumentDefinition>,
    route_id: InstalledFixtureInstrumentRouteId,
    source_id: SourceId,
    symbol: SourceIdentifier,
    venue: VenueId,
    metadata_revision: MetadataRevision,
    metadata_digest: EvidenceDigest,
    effective_from: Timestamp,
    exclusive_expires_at: Timestamp,
    runtime_generation: ConnectionGeneration,
}

impl InstalledFixtureDisplayDescriptor {
    pub(super) fn try_new(
        definition: Arc<InstalledFixtureInstrumentDefinition>,
        runtime: &InstalledFixtureDisplaySourceRuntime,
    ) -> Result<Arc<Self>, ServiceError> {
        let Some(exclusive_expires_at) = definition.effective_interval().ends_at() else {
            return Err(ServiceError::Unavailable);
        };
        let key = runtime.key();
        if definition.source_id() != key.source_id()
            || definition.venue_token() != key.venue_id()
            || definition.route_id().runtime_instrument_id() != key.instrument_id()
            || runtime.generation() != key.generation()
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(Arc::new(Self {
            route_id: definition.route_id(),
            source_id: definition.source_id().clone(),
            symbol: SourceIdentifier::try_from(definition.wire_symbol().as_str())
                .map_err(|_error| ServiceError::ResourceExhausted)?,
            venue: definition.venue_token().clone(),
            metadata_revision: definition.source_metadata_revision().clone(),
            metadata_digest: definition.source_metadata_digest(),
            effective_from: definition.effective_interval().starts_at(),
            exclusive_expires_at,
            runtime_generation: runtime.generation(),
            definition,
        }))
    }

    pub(crate) const fn definition(&self) -> &Arc<InstalledFixtureInstrumentDefinition> {
        &self.definition
    }

    pub(crate) const fn route_id(&self) -> InstalledFixtureInstrumentRouteId {
        self.route_id
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn symbol(&self) -> &SourceIdentifier {
        &self.symbol
    }

    pub(crate) const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub(crate) const fn metadata_digest(&self) -> EvidenceDigest {
        self.metadata_digest
    }

    pub(crate) const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }

    pub(crate) const fn exclusive_expires_at(&self) -> Timestamp {
        self.exclusive_expires_at
    }

    pub(crate) const fn runtime_generation(&self) -> ConnectionGeneration {
        self.runtime_generation
    }

    fn matches_key(&self, key: &DisplayMarketKey) -> bool {
        self.source_id == *key.source_id()
            && self.venue == *key.venue_id()
            && self.route_id.runtime_instrument_id() == key.instrument_id()
            && self.runtime_generation == key.generation()
    }
}

/// Bounded fixture-only presentation lease with no conversion into production market evidence.
#[derive(Debug)]
pub(crate) struct InstalledFixturePresentationLease {
    descriptor: Arc<InstalledFixtureDisplayDescriptor>,
    snapshot: DisplayMarketSnapshotLease,
}

impl InstalledFixturePresentationLease {
    fn try_new(
        descriptor: Arc<InstalledFixtureDisplayDescriptor>,
        snapshot: DisplayMarketSnapshotLease,
        at: Timestamp,
    ) -> Result<Self, ServiceError> {
        if at < descriptor.effective_from()
            || at >= descriptor.exclusive_expires_at()
            || !descriptor.matches_key(snapshot.key())
            || snapshot.terminal_failure().is_some()
            || snapshot.trade().is_some()
            || snapshot.status().is_some()
        {
            return Err(ServiceError::Unavailable);
        }
        let quote = snapshot.quote().ok_or(ServiceError::Unavailable)?;
        let provenance = quote.observation().provenance();
        if !matches!(
            quote.availability(),
            DisplayMarketAvailability::Fresh { .. }
        ) || provenance.metadata_revision() != descriptor.metadata_revision()
            || provenance.source_identifier().as_str() != descriptor.source_id().as_str()
            || provenance.generation() != descriptor.runtime_generation()
            || provenance.received_at() >= descriptor.exclusive_expires_at()
            || !matches!(
                quote.observation().payload(),
                DisplayMarketPayload::Quote(_)
            )
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self {
            descriptor,
            snapshot,
        })
    }

    pub(crate) const fn descriptor(&self) -> &Arc<InstalledFixtureDisplayDescriptor> {
        &self.descriptor
    }

    pub(crate) const fn snapshot(&self) -> &DisplayMarketSnapshotLease {
        &self.snapshot
    }
}

/// Cloneable, fixture-only read authority detached from registry entry ownership.
///
/// The registry revalidates the exact generation both before and after each bounded read. This
/// handle cannot enumerate the production display directory or construct production evidence.
#[derive(Clone, Debug)]
pub(super) struct InstalledFixturePresentationAuthority {
    descriptor: Arc<InstalledFixtureDisplayDescriptor>,
    read: InstalledFixtureDisplayReadAuthority,
}

impl InstalledFixturePresentationAuthority {
    pub(super) async fn presentation(
        &self,
        at: Timestamp,
        cancellation: &tokio_util::sync::CancellationToken,
        deadline: std::time::Instant,
    ) -> Result<InstalledFixturePresentationLease, ServiceError> {
        if at >= self.descriptor.exclusive_expires_at()
            || system_timestamp()? >= self.descriptor.exclusive_expires_at()
        {
            return Err(ServiceError::Unavailable);
        }
        let snapshot = self
            .read
            .snapshot(at, cancellation, deadline)
            .await
            .map_err(map_fixture_runtime_error)?;
        if system_timestamp()? >= self.descriptor.exclusive_expires_at() {
            return Err(ServiceError::Unavailable);
        }
        InstalledFixturePresentationLease::try_new(Arc::clone(&self.descriptor), snapshot, at)
    }
}

/// Runtime owner that exposes only health, fixture presentation, and bounded shutdown.
#[derive(Debug)]
pub(super) struct InstalledFixtureMarketRuntime {
    descriptor: Arc<InstalledFixtureDisplayDescriptor>,
    read: InstalledFixtureDisplayReadAuthority,
    runtime: InstalledFixtureDisplaySourceRuntime,
}

impl InstalledFixtureMarketRuntime {
    pub(super) fn new(
        descriptor: Arc<InstalledFixtureDisplayDescriptor>,
        runtime: InstalledFixtureDisplaySourceRuntime,
    ) -> Self {
        let read = runtime.read_authority();
        debug_assert!(descriptor.matches_key(read.key()));
        Self {
            descriptor,
            read,
            runtime,
        }
    }

    pub(super) fn is_healthy(&self) -> bool {
        self.runtime.is_healthy()
    }

    pub(super) fn is_published_healthy(&self) -> bool {
        self.runtime.is_published_healthy()
    }

    pub(super) fn generation(&self) -> ConnectionGeneration {
        self.runtime.generation()
    }

    pub(super) fn descriptor(&self) -> &Arc<InstalledFixtureDisplayDescriptor> {
        &self.descriptor
    }

    pub(super) fn presentation_authority(&self) -> InstalledFixturePresentationAuthority {
        InstalledFixturePresentationAuthority {
            descriptor: Arc::clone(&self.descriptor),
            read: self.read.clone(),
        }
    }

    pub(super) fn terminal_notification(&self) -> tokio_util::sync::CancellationToken {
        self.runtime.terminal_notification()
    }

    pub(super) fn admit_reads(&self) -> bool {
        self.runtime.admit_reads()
    }

    pub(super) fn begin_shutdown(&self) {
        self.runtime.begin_shutdown();
    }

    pub(super) async fn shutdown(self) -> Result<(), ServiceError> {
        self.runtime
            .shutdown()
            .await
            .map_err(map_fixture_runtime_error)
    }
}

pub(super) fn map_fixture_runtime_error(
    error: InstalledFixtureDisplayRuntimeError,
) -> ServiceError {
    tracing::error!(%error, "installed fixture display runtime failed");
    match error {
        InstalledFixtureDisplayRuntimeError::Expired
        | InstalledFixtureDisplayRuntimeError::Unavailable => ServiceError::Unavailable,
        InstalledFixtureDisplayRuntimeError::DisplayRead(
            crate::live_source::display_market::DisplayMarketReadError::Cancelled,
        ) => ServiceError::Cancelled,
        InstalledFixtureDisplayRuntimeError::DisplayRead(
            crate::live_source::display_market::DisplayMarketReadError::Deadline,
        )
        | InstalledFixtureDisplayRuntimeError::Deadline => ServiceError::DeadlineExceeded,
        InstalledFixtureDisplayRuntimeError::Cancelled => ServiceError::Cancelled,
        _ => ServiceError::Unavailable,
    }
}

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_error| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
