//! This module contains the definition and implementation of [`Frame`] and [`FrameGroup`], which
//! are types that represent the buffer frames that the buffer pool manager is in charge of.
//!
//! A [`Frame`] is intended to hold [`PAGE_SIZE`] bytes of data, and is also intended to be shared
//! with the the kernel to avoid unnecessary `memcpy`s from the kernel's internal buffers into
//! user-space buffers.
//!
//! A [`FrameGroup`] instance groups [`Frame`]s together so that eviction algorithms can be run on
//! pre-determined groups of frames without having to manage which logical pages are in memory or
//! not in memory.

use super::eviction::EvictionState;
use crate::page::{PageRef, WritePageGuard, PAGE_SIZE};
use async_channel::{Receiver, Sender};
use futures::future;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub struct Frame {
    /// The buffer that this `Frame` holds ownership over.
    ///
    /// Since `Frame` is not [`Clone`]able, this `Frame` is guaranteed to have exclusive access to
    /// the mutable buffer.
    buf: &'static mut [u8],

    /// A pointer to the [`FrameGroup`] that this `Frame` belongs to.
    frame_group: Arc<FrameGroup>,

    /// The index of the frame group that refers to this `Frame`.
    group_index: usize,
}

impl Frame {
    /// Creates a new and owned [`Frame`].
    pub fn new(buf: &'static mut [u8], frame_group: Arc<FrameGroup>, group_index: usize) -> Self {
        assert_eq!(buf.len(), PAGE_SIZE);
        Self {
            buf,
            frame_group,
            group_index,
        }
    }

    /// Gets the frame group ID.
    pub fn frame_group_id(&self) -> usize {
        self.frame_group.id
    }

    /// Gets the index of the frame group that refers to this `Frame`.
    pub fn group_index(&self) -> usize {
        self.group_index
    }

    /// Returns a raw pointer to this frame's buffer.
    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    /// Returns a mutable pointer to this frame's buffer.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    /// Gets a `Frame`'s eviction state (via its [`FrameGroup`]).
    pub fn eviction_state(&self) -> &EvictionState {
        &self.frame_group.frame_states[self.group_index]
    }

    /// Returns a reference to the owner of this page, if this `Frame` actually has an owner.
    pub fn get_page_owner(&self) -> Option<PageRef> {
        self.eviction_state().get_owner()
    }

    /// Sets the frame's owner as the given page.
    pub fn set_page_owner(&self, page: PageRef) {
        self.eviction_state().set_owner(page)
    }

    /// Changes the `Frame`'s state to [`Cold`](super::eviction::FrameTemperature::Cold) and returns
    /// the previous owner of the current `Frame`, if it had a [`PageRef`] owner in the first place.
    pub fn evict_page_owner(&self) -> Option<PageRef> {
        self.eviction_state().evict()
    }

    /// Records an access on the current `Frame`.
    pub fn record_access(&self) {
        self.eviction_state().record_access()
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

/// The number of frames in a [`FrameGroup`].
pub const FRAME_GROUP_SIZE: usize = 64;

/// A fixed group of [`Frame`]s.
///
/// This type is intended to facilitate finding a random group of frames to run the eviction
/// algorithm over.
#[derive(Debug)]
pub struct FrameGroup {
    /// The unique group ID of this `FrameGroup`.
    id: usize,

    /// The states of the [`Frame`]s that belong to this `FrameGroup`.
    frame_states: Box<[EvictionState]>,

    /// An asynchronous channel of free [`Frame`]s.
    free_frames: (Sender<Frame>, Receiver<Frame>),
}

/// A reference-counted reference to a [`FrameGroup`].
pub type FrameGroupRef = Arc<FrameGroup>;

impl FrameGroup {
    /// Creates a new `FrameGroup` given an owned vector of static mutable buffers and an ID.
    ///
    /// # Panics
    ///
    /// This function will panic if the length of the input slice `buffers` is not equal to
    /// [`FRAME_GROUP_SIZE`].
    pub fn new(buffers: Vec<&'static mut [u8]>, frame_group_id: usize) -> FrameGroupRef {
        assert_eq!(buffers.len(), FRAME_GROUP_SIZE);

        let frame_states: Vec<EvictionState> = (0..FRAME_GROUP_SIZE)
            .map(|_| EvictionState::default())
            .collect();
        let frame_states = frame_states.into_boxed_slice();
        assert_eq!(frame_states.len(), FRAME_GROUP_SIZE);

        let (rx, tx) = async_channel::bounded(FRAME_GROUP_SIZE);

        let frame_group = Arc::new(Self {
            id: frame_group_id,
            frame_states,
            free_frames: (rx.clone(), tx),
        });

        // All free frames should start inside the `free_frames` channel
        buffers
            .into_iter()
            .enumerate()
            .map(|(i, buf)| Frame::new(buf, frame_group.clone(), i))
            .for_each(|frame| {
                rx.send_blocking(frame).unwrap();
            });

        frame_group
    }

    /// Gets a free frame in this `FrameGroup`.
    ///
    /// This function will evict other frames in this `FrameGroup` if there are no free frames
    /// available.
    pub async fn get_free_frame(&self, page: PageRef) -> Frame {
        loop {
            if let Ok(frame) = self.free_frames.1.try_recv() {
                self.frame_states[frame.group_index].set_owner(page);
                return frame;
            }

            self.cool().await;
        }
    }

    /// Runs the second chance / clock algorithm on all of the [`Frame`]s in this `FrameGroup`, and
    /// then evicts all of the frames that have been cooled twice.
    pub async fn cool(&self) {
        let mut eviction_pages: Vec<PageRef> = Vec::with_capacity(FRAME_GROUP_SIZE);

        // Cool all of the frames, recording a frame if it is already cool
        for frame_temperature in self.frame_states.iter() {
            if let Some(page) = frame_temperature.cool() {
                eviction_pages.push(page);
            }
        }

        if eviction_pages.is_empty() {
            return;
        }

        // Attempt to evict all of the already cool frames
        let futures: Vec<_> = eviction_pages
            .iter()
            .map(|page| {
                // Return a future that can be run concurrently with other eviction futures later
                async move {
                    // If we cannot get the write guard immediately, then someone else has it and we
                    // don't need to evict this frame now.
                    if let Ok(guard) = page.inner.try_write() {
                        let write_guard = WritePageGuard::new(page.pid, guard);

                        let frame = write_guard.evict().await;

                        self.free_frames.0.send(frame).await.unwrap();
                    }
                }
            })
            .collect();

        future::join_all(futures).await;
    }
}
