//! Checked language-visible retained-allocation layout formulas.
//!
//! These formulas model the Rust 1.97 `Arc` header and pointee layout. They deliberately exclude
//! allocator metadata, size classes, fragmentation, native stacks, kernel objects, and RSS.

use std::alloc::Layout;
use std::fmt;
use std::sync::atomic::AtomicUsize;

#[repr(C)]
struct ArcAllocationHeader {
    strong: AtomicUsize,
    weak: AtomicUsize,
}

/// A checked retained-allocation layout could not be represented by `usize`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetainedLayoutError {
    /// The header/pointee allocation layout overflowed.
    LayoutOverflow,
    /// Additional owned dynamic bytes overflowed the composed allocation size.
    DynamicAllocationOverflow,
}

impl fmt::Display for RetainedLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LayoutOverflow => formatter.write_str("retained Arc allocation layout overflow"),
            Self::DynamicAllocationOverflow => {
                formatter.write_str("retained Arc dynamic allocation size overflow")
            }
        }
    }
}

impl std::error::Error for RetainedLayoutError {}

/// Returns the checked Rust-visible allocation bytes for one `Arc<T>` and `T`-owned dynamic data.
///
/// `pointee_dynamic_bytes` contains allocations owned by the sized pointee but not embedded in its
/// inline value. It must not contain allocator metadata or add the `Arc` header a second time.
///
/// # Errors
///
/// Returns [`RetainedLayoutError::LayoutOverflow`] when the header and pointee layout cannot be
/// composed, or [`RetainedLayoutError::DynamicAllocationOverflow`] when owned dynamic bytes cannot
/// be added to that layout.
pub fn checked_arc_value_allocation_bytes<T>(
    pointee_dynamic_bytes: usize,
) -> Result<usize, RetainedLayoutError> {
    let (allocation, _) = Layout::new::<ArcAllocationHeader>()
        .extend(Layout::new::<T>())
        .map_err(|_| RetainedLayoutError::LayoutOverflow)?;
    allocation
        .pad_to_align()
        .size()
        .checked_add(pointee_dynamic_bytes)
        .ok_or(RetainedLayoutError::DynamicAllocationOverflow)
}

/// Returns the checked Rust-visible allocation bytes for one right-sized `Arc<[u8]>`.
///
/// # Errors
///
/// Returns [`RetainedLayoutError::LayoutOverflow`] when the byte-slice or composed allocation
/// layout cannot be represented.
pub fn checked_arc_bytes_allocation_bytes(length: usize) -> Result<usize, RetainedLayoutError> {
    let bytes = Layout::array::<u8>(length).map_err(|_| RetainedLayoutError::LayoutOverflow)?;
    let (allocation, _) = Layout::new::<ArcAllocationHeader>()
        .extend(bytes)
        .map_err(|_| RetainedLayoutError::LayoutOverflow)?;
    Ok(allocation.pad_to_align().size())
}

/// Returns the checked Rust-visible allocation bytes for one right-sized `Arc<str>`.
///
/// UTF-8 validity is an owner invariant; the allocation layout is the same one-byte-aligned
/// unsized tail layout as `Arc<[u8]>`.
///
/// # Errors
///
/// Returns [`RetainedLayoutError::LayoutOverflow`] when the composed allocation layout cannot be
/// represented.
pub fn checked_arc_str_allocation_bytes(length: usize) -> Result<usize, RetainedLayoutError> {
    checked_arc_bytes_allocation_bytes(length)
}
