//! Failure-atomic admission and publication of immutable custom query sources.

use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};
use datafusion::prelude::SessionContext;

use super::scan::{ImmutableSourceStorage, ImmutableSourceTable};
use super::{PinnedObjectStoreRegistry, PinnedReadOnlyObjectStore};
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::parquet_store::VerifiedPinnedObject;
use crate::{PinnedDataset, QueryError};

const REGISTRATION_FIXED_RECEIPT: usize = 32 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PinnedRegistrationAllocation {
    retained: usize,
    construction_peak: usize,
    objects: usize,
}

/// Complete source construction receipt acquired before capture and publication.
#[derive(Debug)]
pub(super) struct PinnedRegistrationAdmission {
    allocation: PinnedRegistrationAllocation,
    reservation: Arc<MemoryReservation>,
}

impl PinnedRegistrationAdmission {
    pub(super) fn capture_lease(&self) -> Arc<MemoryReservation> {
        Arc::clone(&self.reservation)
    }

    pub(super) fn reserve_for_dataset(
        schema: &SchemaRef,
        dataset: &PinnedDataset,
        reservation: MemoryReservation,
        limit: u64,
    ) -> Result<Self, QueryError> {
        if schema.fields().is_empty() {
            return Err(QueryError::InvalidSource);
        }
        let mut objects = 0_usize;
        let mut references = 0_usize;
        for object in dataset.objects() {
            let reference_bytes = object.relative_reference().len();
            if reference_bytes == 0 {
                return Err(QueryError::InvalidSource);
            }
            objects = objects.checked_add(1).ok_or(QueryError::SizeOverflow)?;
            references = references
                .checked_add(reference_bytes)
                .ok_or(QueryError::SizeOverflow)?;
        }
        if objects == 0 {
            return Err(QueryError::InvalidSource);
        }
        let object_inline = objects
            .checked_mul(size_of::<VerifiedPinnedObject>())
            .ok_or(QueryError::SizeOverflow)?;
        let lookup_inline = objects
            .checked_mul(size_of::<usize>())
            .ok_or(QueryError::SizeOverflow)?;
        let retained = REGISTRATION_FIXED_RECEIPT
            .checked_add(object_inline)
            .and_then(|value| value.checked_add(lookup_inline))
            .and_then(|value| value.checked_add(references))
            .ok_or(QueryError::SizeOverflow)?;
        // Metadata is separately admitted from each exact footer before decoding and retained
        // by the verified file. This receipt owns only registration/capture containers.
        let construction_peak = retained
            .checked_add(references)
            .and_then(|value| value.checked_add(object_inline))
            .and_then(|value| value.checked_add(lookup_inline))
            .ok_or(QueryError::SizeOverflow)?;
        super::super::budget::reserve_memory(&reservation, construction_peak, limit)?;
        Ok(Self {
            allocation: PinnedRegistrationAllocation {
                retained,
                construction_peak,
                objects,
            },
            reservation: Arc::new(reservation),
        })
    }
}

/// Receipt shared by every object-store/table/plan reference to registered state.
#[derive(Debug)]
pub(super) struct RetainedPinnedMetadata {
    pub(super) reservation: Arc<MemoryReservation>,
}

/// A complete source graph which has not crossed the SessionContext boundary.
pub(super) struct PinnedRegistrationBundle {
    store: Arc<PinnedReadOnlyObjectStore>,
    table: Arc<ImmutableSourceTable>,
}

impl PinnedRegistrationBundle {
    pub(super) fn complete(
        schema: SchemaRef,
        mut verified: Vec<VerifiedPinnedObject>,
        memory_pool: Arc<dyn MemoryPool>,
        supervisor: BlockingIoSupervisor,
        admission: PinnedRegistrationAdmission,
    ) -> Result<Self, QueryError> {
        if verified.len() != admission.allocation.objects
            || verified.capacity() != admission.allocation.objects
            || verified_retained_bytes(&verified)? != admission.allocation.retained
        {
            return Err(QueryError::DependencyAllocationContract);
        }
        for file in &mut verified {
            file.bind_reader_schema(Arc::clone(&schema))?;
        }
        let mut lookup = Vec::new();
        lookup
            .try_reserve_exact(verified.len())
            .map_err(|_| QueryError::DependencyAllocationContract)?;
        lookup.extend(0..verified.len());
        lookup.sort_unstable_by(|left, right| {
            verified[*left]
                .relative_reference()
                .cmp(verified[*right].relative_reference())
        });
        if lookup.windows(2).any(|pair| {
            verified[pair[0]].relative_reference() >= verified[pair[1]].relative_reference()
        }) {
            return Err(QueryError::InvalidSource);
        }
        let lookup_allocation = lookup.as_ptr();
        let lookup = lookup.into_boxed_slice();
        if lookup.as_ptr() != lookup_allocation {
            return Err(QueryError::DependencyAllocationContract);
        }
        let allocation = verified.as_ptr();
        let verified = verified.into_boxed_slice();
        if verified.as_ptr() != allocation {
            return Err(QueryError::DependencyAllocationContract);
        }
        let files = Arc::new(verified);
        let retained_metadata = Arc::new(RetainedPinnedMetadata {
            reservation: admission.reservation,
        });
        let storage = Arc::new(ImmutableSourceStorage::pinned(
            Arc::clone(&files),
            Arc::clone(&retained_metadata),
        ));
        let table = Arc::new(ImmutableSourceTable::try_new(
            schema,
            storage,
            Arc::clone(&memory_pool),
            supervisor.clone(),
        )?);
        let store = Arc::new(PinnedReadOnlyObjectStore::new_exact(
            files,
            lookup,
            memory_pool,
            supervisor,
            Arc::clone(&retained_metadata),
        ));
        retained_metadata
            .reservation
            .try_resize(admission.allocation.retained)
            .map_err(|_| QueryError::DependencyAllocationContract)?;
        Ok(Self { store, table })
    }

    pub(super) fn publish(
        self,
        context: &SessionContext,
        table_name: &str,
        registry: &PinnedObjectStoreRegistry,
    ) -> Result<(), QueryError> {
        if context.table_exist(table_name)? {
            return Err(QueryError::InvalidSource);
        }
        let store: Arc<dyn datafusion::object_store::ObjectStore> = self.store;
        let table: Arc<dyn TableProvider> = self.table;
        let install = registry.reserve_install(store)?;
        match context.register_table(table_name, Arc::clone(&table)) {
            Ok(None) => {
                install.commit();
                Ok(())
            }
            Ok(Some(previous)) => {
                let replaced = context.register_table(table_name, previous)?;
                if replaced
                    .as_ref()
                    .is_none_or(|replaced| !Arc::ptr_eq(replaced, &table))
                {
                    return Err(QueryError::DependencyAllocationContract);
                }
                Err(QueryError::DependencyAllocationContract)
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn verified_retained_bytes(verified: &[VerifiedPinnedObject]) -> Result<usize, QueryError> {
    let inline = verified
        .len()
        .checked_mul(size_of::<VerifiedPinnedObject>())
        .ok_or(QueryError::SizeOverflow)?;
    let lookup_inline = verified
        .len()
        .checked_mul(size_of::<usize>())
        .ok_or(QueryError::SizeOverflow)?;
    verified.iter().try_fold(
        REGISTRATION_FIXED_RECEIPT
            .checked_add(inline)
            .and_then(|value| value.checked_add(lookup_inline))
            .ok_or(QueryError::SizeOverflow)?,
        |total, object| {
            total
                .checked_add(object.relative_reference().len())
                .ok_or(QueryError::SizeOverflow)
        },
    )
}
