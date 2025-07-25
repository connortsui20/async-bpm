//! This module contains the type definitions and implementation for the [`Frame`] struct.
//!
//! A [`Frame`] is intended to hold [`PAGE_SIZE`] bytes of data, and is also intended to be shared
//! with the the kernel to avoid unnecessary `memcpy`s from the kernel's internal buffers into
//! user-space buffers.

use crate::storage::frame_group::{EvictionState, FrameGroup, FRAME_GROUP_SIZE};
use crate::{
    bpm::BufferPoolManager,
    page::{Page, PAGE_SIZE},
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio_uring::buf::{IoBuf, IoBufMut};

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub(crate) struct Frame {
    /// The unique ID of this `Frame`.
    ///
    /// Each `Frame` is assigned a monotonically increasing ID, where every chunk of
    /// [`FRAME_GROUP_SIZE`] `Frame`s represent a single [`FrameGroup`].
    frame_id: usize,

    /// The owner of this `Frame`, if one exists.
    ///
    /// If a [`Page`] "owns" this `Frame` (the `Frame` holds the [`Page`]s data), then it is the
    /// responsibility of the [`Page`] to ensure that they place an [`Arc`] into this field via
    /// [`replace_page_owner`](Self::replace_page_owner).
    page_owner: Option<Arc<Page>>,

    /// A flag representing if the `Frame` is dirty or not.
    ///
    /// If we never modify a [`Page`] that the `Frame` holds, then we don't need to worry about
    /// writing out updates to storage. With this flag, we only incur the I/O operation when
    /// absolutely necessary.
    dirty: bool,

    /// The buffer that this `Frame` holds ownership over.
    ///
    /// Since `Frame` is not [`Clone`]able, this `Frame` is guaranteed to have exclusive access to
    /// the mutable buffer.
    buf: &'static mut [u8],
}

impl Frame {
    /// Creates a new `Frame` given a static mutable buffer and a frame ID.
    ///
    /// All `Frame`s are initialized without any page owner.
    pub(crate) fn new(frame_id: usize, buf: &'static mut [u8]) -> Self {
        Self {
            frame_id,
            buf,
            dirty: false,
            page_owner: None,
        }
    }

    /// Gets the unique frame ID of the frame.
    pub(crate) fn frame_id(&self) -> usize {
        self.frame_id
    }

    /// Gets the frame group ID of the group that this frame belongs to.
    pub(crate) fn group_id(&self) -> usize {
        self.frame_id / FRAME_GROUP_SIZE
    }

    /// Gets an [`Arc`] to the [`FrameGroup`] that this frame belongs to.
    pub(crate) fn group(&self) -> Arc<FrameGroup> {
        let bpm = BufferPoolManager::get();

        bpm.get_frame_group(self.group_id())
    }

    /// Replaces the owning [`Page`] of this `Frame` with another [`Page`].
    pub(crate) fn replace_page_owner(&mut self, page: Arc<Page>) -> Option<Arc<Page>> {
        self.page_owner.replace(page)
    }

    /// Replaces the owning [`Page`] of this `Frame` with `None`.
    pub(crate) fn evict_page_owner(&mut self) -> Option<Arc<Page>> {
        self.page_owner.take()
    }

    /// Updates the eviction state after this frame has been accessed.
    ///
    /// This function will simply update the [`EvictionState`] of the `Frame` to
    /// [`Hot`](EvictionState::Hot).
    pub(crate) fn record_access(&self, page: Arc<Page>) {
        let group = self.group();
        let index = self.frame_id % FRAME_GROUP_SIZE;

        let mut eviction_guard = group
            .eviction_states
            .lock()
            .expect("Fatal: `EvictionState` lock was poisoned somehow");

        eviction_guard[index] = EvictionState::Hot(page.clone());
    }

    /// Checks if the dirty bit is set.
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Sets the dirty bit.
    pub(crate) fn set_dirty(&mut self) {
        self.dirty = true;
    }

    /// Clears the dirty bit.
    pub(crate) fn clear_dirty(&mut self) {
        self.dirty = false;
    }
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl DerefMut for Frame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf
    }
}

/// # Safety
///
/// The safety contract for `IoBuf` is as follows:
/// > Buffers passed to `io-uring` operations must reference a stable memory region. While the
/// > runtime holds ownership to a buffer, the pointer returned by `stable_ptr` must remain valid
/// > even if the `IoBuf` value is moved.
///
/// Since we only use a static reference to correctly allocated memory, all operations are safe.
unsafe impl IoBuf for Frame {
    fn stable_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        PAGE_SIZE
    }

    fn bytes_total(&self) -> usize {
        PAGE_SIZE
    }
}

/// # Safety
///
/// The safety contract for `IoBufMut` is as follows:
/// > Buffers passed to `io-uring` operations must reference a stable memory region. While the
/// > runtime holds ownership to a buffer, the pointer returned by `stable_mut_ptr` must remain
/// > valid even if the `IoBufMut` value is moved.
///
/// Since we only use a static reference to correctly allocated memory, all operations are safe.
unsafe impl IoBufMut for Frame {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, _pos: usize) {
        // All bytes are initialized on allocation, so this function is a no-op.
    }
}
