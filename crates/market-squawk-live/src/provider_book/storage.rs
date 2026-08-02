//! Fixed-capacity provider-book storage and shard-owned mutation scratch.

use std::cmp::Ordering;
use std::sync::Arc;

use market_squawk_domain::{PriceTicks, QuantityLots, SourceIdentifier};
use market_squawk_sources::ProviderBookLevel;
use rust_decimal::Decimal;

use super::ProviderBookError;
use crate::BookSide;
use crate::integrity::ExactChecksumLevel;

/// Exact provider decimal text stored inline so retained book memory does not depend on `String`
/// allocator capacity. The source grammar is strictly smaller than this domain identity bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExactDecimalLexeme {
    bytes: [u8; SourceIdentifier::MAX_LENGTH],
    len: u16,
    decimal: Decimal,
}

impl ExactDecimalLexeme {
    fn try_from_provider(
        value: &market_squawk_sources::ProviderDecimalLexeme,
    ) -> Result<Self, ProviderBookError> {
        let source = value.as_str().as_bytes();
        let len = u16::try_from(source.len()).map_err(|_| ProviderBookError::Allocation)?;
        let target = usize::from(len);
        if target > SourceIdentifier::MAX_LENGTH {
            return Err(ProviderBookError::Allocation);
        }
        let mut bytes = [0_u8; SourceIdentifier::MAX_LENGTH];
        bytes[..target].copy_from_slice(source);
        Ok(Self {
            bytes,
            len,
            decimal: value.decimal(),
        })
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    pub(crate) const fn decimal(&self) -> Decimal {
        self.decimal
    }
}

/// Immutable fixed-size exact provider level shared by unchanged active/candidate entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactProviderLevel {
    price: ExactDecimalLexeme,
    quantity: ExactDecimalLexeme,
}

impl ExactProviderLevel {
    pub(super) fn try_from_provider(value: &ProviderBookLevel) -> Result<Self, ProviderBookError> {
        Ok(Self {
            price: ExactDecimalLexeme::try_from_provider(value.price().value())?,
            quantity: ExactDecimalLexeme::try_from_provider(value.quantity().value())?,
        })
    }

    pub(crate) const fn price(&self) -> &ExactDecimalLexeme {
        &self.price
    }

    pub(crate) const fn quantity(&self) -> &ExactDecimalLexeme {
        &self.quantity
    }
}

impl ExactChecksumLevel for ExactProviderLevel {
    fn price_bytes(&self) -> &[u8] {
        self.price.as_bytes()
    }

    fn quantity_bytes(&self) -> &[u8] {
        self.quantity.as_bytes()
    }

    fn price_decimal(&self) -> Decimal {
        self.price.decimal()
    }

    fn quantity_decimal(&self) -> Decimal {
        self.quantity.decimal()
    }
}

#[derive(Clone, Debug)]
pub(super) struct UnifiedBookLevel {
    pub(super) price: PriceTicks,
    pub(super) quantity: QuantityLots,
    pub(super) exact: Arc<ExactProviderLevel>,
}

/// Unsafe-free fixed logical backing. The boxed slot count is the configured capacity; no
/// allocator-selected spare capacity is observable or used by the live state machine.
#[derive(Debug)]
pub(super) struct FixedBuffer<T> {
    slots: Box<[Option<T>]>,
    len: usize,
}

impl<T> FixedBuffer<T> {
    pub(super) fn try_new(capacity: usize) -> Result<Self, ProviderBookError> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| ProviderBookError::Allocation)?;
        slots.resize_with(capacity, || None);
        Ok(Self {
            slots: slots.into_boxed_slice(),
            len: 0,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn clear(&mut self) {
        for slot in &mut self.slots[..self.len] {
            *slot = None;
        }
        self.len = 0;
    }

    pub(super) fn push(&mut self, value: T) -> Result<(), ProviderBookError> {
        let slot = self
            .slots
            .get_mut(self.len)
            .ok_or(ProviderBookError::Allocation)?;
        *slot = Some(value);
        self.len += 1;
        Ok(())
    }

    pub(super) fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        self.slots.get(index).and_then(Option::as_ref)
    }

    pub(super) fn iter(&self) -> FixedBufferIter<'_, T> {
        FixedBufferIter {
            slots: self.slots[..self.len].iter(),
            remaining: self.len,
        }
    }

    pub(super) fn sort_initialized_by<F>(&mut self, mut compare: F)
    where
        F: FnMut(&T, &T) -> Ordering,
    {
        self.slots[..self.len].sort_unstable_by(|left, right| match (left, right) {
            (Some(left), Some(right)) => compare(left, right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        });
    }
}

pub(super) struct FixedBufferIter<'a, T> {
    slots: std::slice::Iter<'a, Option<T>>,
    remaining: usize,
}

impl<'a, T> Iterator for FixedBufferIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.slots.next()?.as_ref()?;
        self.remaining = self.remaining.saturating_sub(1);
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<T> ExactSizeIterator for FixedBufferIter<'_, T> {
    fn len(&self) -> usize {
        self.remaining
    }
}

/// Two fixed-capacity buffers. Only `active` is published; `candidate` is reusable transaction
/// scratch. A successful transaction swaps them and clears the former committed image.
#[derive(Debug)]
pub(super) struct SideBuffers {
    pub(super) active: FixedBuffer<UnifiedBookLevel>,
    pub(super) candidate: FixedBuffer<UnifiedBookLevel>,
}

impl SideBuffers {
    pub(super) fn try_new(capacity: usize) -> Result<Self, ProviderBookError> {
        Ok(Self {
            active: FixedBuffer::try_new(capacity)?,
            candidate: FixedBuffer::try_new(capacity)?,
        })
    }

    pub(super) fn clear_candidate(&mut self) {
        self.candidate.clear();
    }

    pub(super) fn commit(&mut self) {
        std::mem::swap(&mut self.active, &mut self.candidate);
        self.candidate.clear();
    }

    pub(super) fn push_candidate(
        &mut self,
        level: UnifiedBookLevel,
    ) -> Result<(), ProviderBookError> {
        self.candidate.push(level)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) struct NormalizedChange {
    pub(super) side: BookSide,
    pub(super) price: PriceTicks,
    pub(super) quantity: QuantityLots,
    pub(super) exact: ExactProviderLevel,
    pub(super) ordinal: usize,
}

/// Shard-owned, single-writer normalization and dedupe storage.
///
/// It is constructed before the actor starts and reused for every route/stream on that shard.
/// No normalized/update/rollback allocation occurs while applying a book event.
#[derive(Debug)]
pub(crate) struct BookProcessingScratch {
    pub(super) changes: FixedBuffer<NormalizedChange>,
    maximum_items: usize,
}

impl BookProcessingScratch {
    pub(crate) fn try_new(maximum_items: usize) -> Result<Self, ProviderBookError> {
        if maximum_items == 0 || maximum_items > crate::MAX_BOOK_MESSAGE_ITEMS {
            return Err(ProviderBookError::InvalidScratchCapacity {
                requested: maximum_items,
                maximum: crate::MAX_BOOK_MESSAGE_ITEMS,
            });
        }
        Ok(Self {
            changes: FixedBuffer::try_new(maximum_items)?,
            maximum_items,
        })
    }

    pub(crate) const fn maximum_items(&self) -> usize {
        self.maximum_items
    }

    pub(super) fn clear(&mut self) {
        self.changes.clear();
    }

    pub(super) fn push(&mut self, change: NormalizedChange) -> Result<(), ProviderBookError> {
        if self.changes.len() >= self.maximum_items {
            return Err(ProviderBookError::ScratchCapacityExceeded {
                observed: self.changes.len().saturating_add(1),
                maximum: self.maximum_items,
            });
        }
        self.changes.push(change)
    }

    #[cfg(test)]
    pub(super) fn observed_backing_bytes(&self) -> u64 {
        (self.changes.capacity() as u64) * (std::mem::size_of::<Option<NormalizedChange>>() as u64)
    }
}
