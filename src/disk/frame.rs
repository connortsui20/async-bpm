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
    eviction::{Temperature, TemperatureState},
};
use crate::page::{PageRef, WritePageGuard};
use arc_swap::ArcSwapOption;
use async_channel::{Receiver, Sender};
use futures::future;
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
    pub(crate) owner: ArcSwapOption<PageRef>,

    /// The state of the frame with respect to its eviction.
    pub(crate) eviction_state: Temperature,
}

/// A reference-counted reference to a [`Frame`].
pub type FrameRef = Arc<Frame>;

impl Frame {
    /// Creates a new and owned [`Frame`] given a static [`IoSliceMut`].
    pub fn new(ioslice: IoSliceMut<'static>) -> Self {
        Self {
            buf: ioslice,
            owner: ArcSwapOption::const_empty(),
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
    pub(crate) frames: [FrameRef; FRAME_GROUP_SIZE],

    /// An asynchronous channel of free frames.
    pub(crate) free_frames: (Sender<FrameRef>, Receiver<FrameRef>),
}

/// A reference-counted reference to a [`FrameGroup`].
pub type FrameGroupRef = Arc<FrameGroup>;

impl FrameGroup {
    /// Gets a free frame in this `FrameGroup`.
    ///
    /// This function will evict other frames in this `FrameGroup` if there are no free frames
    /// available.
    pub async fn get_free_frame(&self) -> FrameRef {
        loop {
            if let Ok(frame) = self.free_frames.1.try_recv() {
                return frame;
            }

            self.cool().await;
        }
    }

    pub async fn cool(&self) {
        let mut evictions = Vec::with_capacity(FRAME_GROUP_SIZE);

        for frame in &self.frames {
            match frame.eviction_state.load(Ordering::Acquire) {
                TemperatureState::Cold => (),
                TemperatureState::Cool => evictions.push(frame),
                TemperatureState::Hot => frame
                    .eviction_state
                    .store(TemperatureState::Cool, Ordering::Release),
            }
        }

        let pages: Vec<PageRef> = evictions
            .into_iter()
            .map(|eviction_frame| {
                eviction_frame
                    .owner
                    .load()
                    .deref()
                    .as_ref()
                    .expect("WritePageGuard protects a Frame that does not have an Page Owner")
                    .deref()
                    .clone()
            })
            .collect();

        let dmh = DISK_MANAGER.get().unwrap().create_handle();

        let futures: Vec<_> = pages
            .iter()
            .map(|page| {
                let dmh = dmh.clone();

                async move {
                    if let Ok(guard) = page.inner.try_write() {
                        let write_guard = WritePageGuard::new(page.pid, guard, dmh);

                        let frame = write_guard.evict().await;

                        self.free_frames.0.send(Arc::new(frame)).await.unwrap();
                    }
                }
            })
            .collect();

        future::join_all(futures).await;
    }
}
