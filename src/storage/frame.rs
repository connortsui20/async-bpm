//! This module contains the type definiton and implementation for the [`Frame`] struct.
//!
//! TODO more docs.

use crate::storage::frame_group::{EvictionState, FrameGroup, FRAME_GROUP_SIZE};
use crate::{
    bpm::BufferPoolManager,
    page::{Page, PAGE_SIZE},
};
use std::sync::atomic::AtomicBool;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

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

    /// The buffer that this `Frame` holds ownership over.
    ///
    /// Since `Frame` is not [`Clone`]able, this `Frame` is guaranteed to have exclusive access to
    /// the mutable buffer.
    pub buf: &'static mut [u8],

    /// Set to true if a RwLock on this frame is downgrading from a write lock to a read lock.
    pub is_downgrading: AtomicBool,
}

impl Frame {
    /// Creates a new `Frame` given a static mutable buffer and a frame ID.
    ///
    /// All `Frame`s are initialized without any page owner.
    pub(crate) fn new(frame_id: usize, buf: &'static mut [u8]) -> Self {
        Self {
            frame_id,
            buf,
            page_owner: None,
            is_downgrading: AtomicBool::new(false),
        }
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
