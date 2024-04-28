//! This module contains the definition and implementation of [`Frame`] and [`FrameGroup`], which
//! are types that represent the buffer frames that the buffer pool manager is in charge of.
//!
//! A [`Frame`] is intended to hold [`PAGE_SIZE`](crate::page::PAGE_SIZE) bytes of data, and is
//! also intended to be shared with the the kernel to avoid unnecessary `memcpy`s from the kernel's
//! internal buffers into user-space buffers.
//!
//! A [`FrameGroup`] instance groups [`Frame`]s together so that eviction algorithms can be run on
//! pre-determined groups of frames without having to manage which logical pages are in memory or
//! not in memory.

use super::eviction::{Temperature, TemperatureState};
use crate::page::PageRef;
use async_channel::{Receiver, Sender};
use std::{
    io::IoSliceMut,
    ops::{Deref, DerefMut},
    sync::{atomic::Ordering, Arc},
};

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub struct Frame {
    /// The buffer that this [`Frame`] holds ownership over.
    ///
    /// Since [`Frame`] is not [`Clone`]able, this [`Frame`] is guaranteed to have sole access to
    /// the inner [`IoSliceMut`].
    pub(crate) buf: IoSliceMut<'static>,

    /// A reference to the page that owns this [`Frame`], if an owner actually exists.
    pub(crate) owner: Option<PageRef>,

    /// The state of the frame with respect to its eviction.
    pub(crate) eviction_state: Temperature,
}

impl Frame {
    /// Creates a new and owned [`Frame`] given a static [`IoSliceMut`].
    pub fn new(ioslice: IoSliceMut<'static>) -> Self {
        Self {
            buf: ioslice,
            owner: None,
            eviction_state: Temperature::new(TemperatureState::Cold),
        }
    }

    /// Updates the eviction state after this frame has been accessed.
    pub fn was_accessed(&self) {
        self.eviction_state
            .store(TemperatureState::Hot, Ordering::Release);
    }
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf.deref()
    }
}

impl DerefMut for Frame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.deref_mut()
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
    /// All of the frames in this frame group.
    pub(crate) frames: [Arc<Frame>; FRAME_GROUP_SIZE],

    /// An asynchronous channel of free frames.
    pub(crate) free_frames: (Sender<Frame>, Receiver<Frame>),
}

impl FrameGroup {
    /// Gets a free frame in this `FrameGroup`.
    ///
    /// This function will evict other frames in this `FrameGroup` if there are no free frames
    /// available.
    pub async fn get_free_frame(&self) -> Frame {
        std::hint::black_box(&self.frames);
        std::hint::black_box(&self.free_frames);
        todo!()
    }
}
