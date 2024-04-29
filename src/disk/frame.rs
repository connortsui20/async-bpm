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

use super::{
    disk_manager::DISK_MANAGER,
    eviction::{FrameTemperature, TemperatureState},
};
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

    /// The group ID of the frame group that this `Frame` belongs to.
    frame_group_id: usize,

    /// The index of the frame group that refers to this `Frame`.
    group_index: usize,
}

/// A reference-counted reference to a [`Frame`].
pub type FrameRef = Arc<Frame>;

impl Frame {
    /// Creates a new and owned [`Frame`].
    fn new(buf: &'static mut [u8], frame_group_id: usize, group_index: usize) -> Self {
        assert_eq!(buf.len(), PAGE_SIZE);
        Self {
            buf,
            frame_group_id,
            group_index,
        }
    }

    /// Gets the frame group ID.
    pub(crate) fn frame_group_id(&self) -> usize {
        self.frame_group_id
    }

    /// Gets the index of the frame group that refers to this `Frame`.
    pub(crate) fn group_index(&self) -> usize {
        self.group_index
    }

    /// Returns a raw pointer to this frame's buffer.
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    /// Returns a mutable pointer to this frame's buffer.
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for Frame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
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
    /// The states of the [`Frame`]s that belong to this `FrameGroup`.
    pub(crate) frame_states: Box<[FrameTemperature]>,

    /// An asynchronous channel of free [`Frame`]s.
    pub(crate) free_frames: (Sender<Frame>, Receiver<Frame>),
}

/// A reference-counted reference to a [`FrameGroup`].
pub type FrameGroupRef = Arc<FrameGroup>;

impl FrameGroup {
    /// Creates a new `FrameGroup` given an iterator of static mutable buffers and an ID.
    pub fn new(buffers: impl Iterator<Item = &'static mut [u8]>, frame_group_id: usize) -> Self {
        let (rx, tx) = async_channel::bounded(FRAME_GROUP_SIZE);

        let frames = buffers
            .enumerate()
            .map(|(i, buf)| Frame::new(buf, frame_group_id, i))
            .for_each(|frame| {
                rx.send_blocking(frame).unwrap();
            });

        let frame_states: Vec<FrameTemperature> = (0..FRAME_GROUP_SIZE)
            .map(|_| FrameTemperature::default())
            .collect();
        let frame_states = frame_states.into_boxed_slice();
        assert_eq!(frame_states.len(), FRAME_GROUP_SIZE);

        Self {
            frame_states,
            free_frames: (rx, tx),
        }
    }

    /// Gets a free frame in this `FrameGroup`.
    ///
    /// This function will evict other frames in this `FrameGroup` if there are no free frames
    /// available.
    pub async fn get_free_frame(&self, page: PageRef) -> Frame {
        loop {
            if let Ok(frame) = self.free_frames.1.try_recv() {
                self.frame_states[frame.group_index].store_owner(page);
                return frame;
            }

            self.cool().await;
        }
    }

    /// Runs the second chance / clock algorithm on all of the [`Frame`]s in this `FrameGroup`.
    pub async fn cool(&self) {
        let dmh = DISK_MANAGER.get().unwrap().create_handle();

        let mut eviction_pages: Vec<PageRef> = Vec::with_capacity(FRAME_GROUP_SIZE);

        // Cool all of the frames, recording a frame if it is already cool
        for frame in self.frame_states.iter() {
            let mut guard = frame.inner.lock().await;
            match guard.deref() {
                TemperatureState::Hot(page) => *guard = TemperatureState::Cool(page.clone()),
                TemperatureState::Cool(page) => {
                    eviction_pages.push(page.clone());
                    *guard = TemperatureState::Cold;
                }
                TemperatureState::Cold => (),
            }
        }

        // Attempt to evict all of the already cool frames
        let futures: Vec<_> = eviction_pages
            .iter()
            .map(|page| {
                // Note that this is cheap to clone
                let dmh = dmh.clone();

                // Return a future that can be run concurrently with other eviction futures later
                async move {
                    // If we cannot get the write guard immediately, then someone else has it and we
                    // don't need to evict this frame now.
                    if let Ok(guard) = page.inner.try_write() {
                        let write_guard = WritePageGuard::new(page.pid, guard, dmh);

                        let frame = write_guard.evict().await;

                        self.free_frames.0.send(frame).await.unwrap();
                    }
                }
            })
            .collect();

        future::join_all(futures).await;
    }
}
