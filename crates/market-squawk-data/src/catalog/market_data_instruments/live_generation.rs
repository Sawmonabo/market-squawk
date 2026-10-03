//! Bounded revocation for the exact identities observed by active native-source selections.

use std::{
    mem::size_of,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use market_squawk_domain::{
    InstrumentId, MarketDataInstrumentDefinition, ProviderInstrumentId, SourceId,
};
use market_squawk_sources::{ProviderNativeIdentityRequest, RegistryError};

use super::MarketDataInstrumentCatalogError;

const COUNTER_ALLOCATION_BYTES: usize = size_of::<AtomicU64>() + 2 * size_of::<usize>();

/// Private catalog-lifetime authority. Mutation and watch capture run under the catalog mutex.
/// Retains only keys observed by live selections, never catalog definitions or a catalog copy.
/// Watch allocation uses the existing catalog result-memory admission limit.
#[derive(Debug)]
pub(in crate::catalog) struct LiveProviderIdentityGenerations {
    lifetime: Arc<()>,
    watches: Mutex<Watches>,
}

#[derive(Debug)]
struct Watches {
    maximum_bytes: usize,
    instruments: Vec<(InstrumentId, Arc<AtomicU64>)>,
    native: Vec<(SourceId, ProviderInstrumentId, Arc<AtomicU64>)>,
    native_bytes: usize,
    selections_since_prune: u16,
}

/// Two exact revocation dimensions plus the lifetime of the catalog that minted them.
#[derive(Debug)]
pub(super) struct LiveProviderIdentityGeneration {
    lifetime: Weak<()>,
    instrument: Weak<AtomicU64>,
    native: Weak<AtomicU64>,
    instrument_generation: u64,
    native_generation: u64,
}

impl LiveProviderIdentityGenerations {
    pub(in crate::catalog) fn new(maximum_bytes: usize) -> Self {
        Self {
            lifetime: Arc::new(()),
            watches: Mutex::new(Watches {
                maximum_bytes,
                instruments: Vec::new(),
                native: Vec::new(),
                native_bytes: 0,
                selections_since_prune: 0,
            }),
        }
    }

    /// Capture under the catalog mutex, before opening the independent resolution snapshot.
    /// This watch is private provisional state: the caller must prove exact current identity
    /// and validate the watch after snapshot completion before returning any live authority.
    pub(super) fn select(
        &self,
        request: &ProviderNativeIdentityRequest,
    ) -> Result<LiveProviderIdentityGeneration, MarketDataInstrumentCatalogError> {
        let mut watches = self
            .watches
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?;
        if watches.selections_since_prune >= 256 {
            watches.prune();
        }
        watches.selections_since_prune += 1;
        let instrument = watches.instrument(request.instrument)?;
        let native = watches.native(&request.namespace, &request.provider_instrument_id)?;
        let instrument_generation = instrument.load(Ordering::Acquire);
        let native_generation = native.load(Ordering::Acquire);
        if instrument_generation == u64::MAX || native_generation == u64::MAX {
            return Err(MarketDataInstrumentCatalogError::RevisionLimitExceeded);
        }
        Ok(LiveProviderIdentityGeneration {
            lifetime: Arc::downgrade(&self.lifetime),
            instrument: Arc::downgrade(&instrument),
            native: Arc::downgrade(&native),
            instrument_generation,
            native_generation,
        })
    }

    /// Revoke before the first SQL write. Rollback may revoke affected selections, but can
    /// never leave an old token live over a committed successor or a new ambiguous identity.
    /// New definitions allocate no watch: only currently observed keys can require revocation.
    pub(super) fn invalidate(
        &self,
        definition: &MarketDataInstrumentDefinition,
    ) -> Result<(), MarketDataInstrumentCatalogError> {
        let watches = self
            .watches
            .try_lock()
            .map_err(|_| MarketDataInstrumentCatalogError::AuthorityUnavailable)?;
        if let Ok(index) = watches
            .instruments
            .binary_search_by_key(&definition.instrument_id(), |(instrument, _)| *instrument)
        {
            advance(&watches.instruments[index].1)?;
        }
        // A different canonical instrument can introduce ambiguity for a watched native ID.
        // All incoming assertions participate, including future intervals and retained history;
        // conservative revocation avoids silently admitting a later time-dependent collision.
        for identity in definition.provider_identities() {
            if let Ok(index) = watches.native.binary_search_by(|(namespace, native, _)| {
                (namespace, native).cmp(&(identity.source_id(), identity.provider_instrument_id()))
            }) {
                advance(&watches.native[index].2)?;
            }
        }
        Ok(())
    }
}

impl LiveProviderIdentityGeneration {
    /// Hot-path check: no mutex, allocation, catalog access, or refresh of immutable evidence.
    pub(super) fn validate(&self) -> Result<(), RegistryError> {
        let _lifetime = self.lifetime.upgrade().ok_or(RegistryError::StaleHandle)?;
        let instrument = self
            .instrument
            .upgrade()
            .ok_or(RegistryError::StaleHandle)?;
        let native = self.native.upgrade().ok_or(RegistryError::StaleHandle)?;
        if self.instrument_generation == u64::MAX
            || self.native_generation == u64::MAX
            || instrument.load(Ordering::Acquire) != self.instrument_generation
            || native.load(Ordering::Acquire) != self.native_generation
        {
            return Err(RegistryError::ProviderIdentitySelectionStale);
        }
        Ok(())
    }

    /// Shared allocations are conservatively charged to every retained selection. Weak handles
    /// themselves are already included in the enclosing token's `size_of` charge.
    pub(super) const fn retained_allocation_bytes(&self) -> usize {
        2 * COUNTER_ALLOCATION_BYTES + 2 * size_of::<usize>()
    }
}

impl Watches {
    fn prune(&mut self) {
        self.instruments.retain(|(_, generation)| {
            Arc::weak_count(generation) != 0 || Arc::strong_count(generation) > 1
        });
        let mut retained_bytes = 0usize;
        self.native.retain(|(namespace, native, generation)| {
            let retained = Arc::weak_count(generation) != 0 || Arc::strong_count(generation) > 1;
            if retained {
                // These bytes were checked at insertion; pruning only reduces the sum.
                retained_bytes += namespace.retained_bytes() + native.retained_bytes();
            }
            retained
        });
        self.native_bytes = retained_bytes;
        self.selections_since_prune = 0;
    }

    fn instrument(
        &mut self,
        instrument: InstrumentId,
    ) -> Result<Arc<AtomicU64>, MarketDataInstrumentCatalogError> {
        match self
            .instruments
            .binary_search_by_key(&instrument, |(key, _)| *key)
        {
            Ok(index) => Ok(Arc::clone(&self.instruments[index].1)),
            Err(_) => {
                self.make_room(0, true)?;
                let index = self
                    .instruments
                    .partition_point(|(key, _)| *key < instrument);
                self.instruments
                    .try_reserve_exact(1)
                    .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                let generation = Arc::new(AtomicU64::new(0));
                self.instruments
                    .insert(index, (instrument, Arc::clone(&generation)));
                Ok(generation)
            }
        }
    }

    fn native(
        &mut self,
        namespace: &SourceId,
        native: &ProviderInstrumentId,
    ) -> Result<Arc<AtomicU64>, MarketDataInstrumentCatalogError> {
        match self
            .native
            .binary_search_by(|(source, symbol, _)| (source, symbol).cmp(&(namespace, native)))
        {
            Ok(index) => Ok(Arc::clone(&self.native[index].2)),
            Err(_) => {
                let bytes = namespace
                    .retained_bytes()
                    .checked_add(native.retained_bytes())
                    .ok_or(MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                self.make_room(bytes, false)?;
                let index = self
                    .native
                    .partition_point(|(source, symbol, _)| (source, symbol) < (namespace, native));
                self.native
                    .try_reserve_exact(1)
                    .map_err(|_| MarketDataInstrumentCatalogError::ResultByteLimitExceeded)?;
                let generation = Arc::new(AtomicU64::new(0));
                self.native.insert(
                    index,
                    (namespace.clone(), native.clone(), Arc::clone(&generation)),
                );
                self.native_bytes += bytes;
                Ok(generation)
            }
        }
    }

    fn make_room(
        &mut self,
        new_dynamic_bytes: usize,
        instrument: bool,
    ) -> Result<(), MarketDataInstrumentCatalogError> {
        if self.require_room(new_dynamic_bytes, instrument).is_err() {
            self.prune();
        }
        self.require_room(new_dynamic_bytes, instrument)
    }

    fn require_room(
        &self,
        new_dynamic_bytes: usize,
        instrument: bool,
    ) -> Result<(), MarketDataInstrumentCatalogError> {
        let error = || MarketDataInstrumentCatalogError::ResultByteLimitExceeded;
        let keys = self
            .instruments
            .len()
            .checked_add(self.native.len())
            .ok_or_else(error)?;
        let instrument_capacity = self.instruments.capacity().max(
            self.instruments
                .len()
                .checked_add(usize::from(instrument))
                .ok_or_else(error)?,
        );
        let native_capacity = self.native.capacity().max(
            self.native
                .len()
                .checked_add(usize::from(!instrument))
                .ok_or_else(error)?,
        );
        let bytes = instrument_capacity
            .checked_mul(size_of::<(InstrumentId, Arc<AtomicU64>)>())
            .and_then(|bytes| {
                bytes.checked_add(native_capacity.checked_mul(size_of::<(
                    SourceId,
                    ProviderInstrumentId,
                    Arc<AtomicU64>,
                )>())?)
            })
            .and_then(|bytes| {
                bytes.checked_add(keys.checked_add(1)?.checked_mul(COUNTER_ALLOCATION_BYTES)?)
            })
            .and_then(|bytes| bytes.checked_add(self.native_bytes))
            .and_then(|bytes| bytes.checked_add(new_dynamic_bytes))
            .ok_or_else(error)?;
        if bytes > self.maximum_bytes {
            return Err(error());
        }
        Ok(())
    }
}

fn advance(generation: &AtomicU64) -> Result<(), MarketDataInstrumentCatalogError> {
    generation
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map(|_| ())
        .map_err(|_| MarketDataInstrumentCatalogError::RevisionLimitExceeded)
}
