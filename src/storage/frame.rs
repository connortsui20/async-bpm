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

use crate::storage::storage_manager::StorageManager;
use crate::{
    bpm::BufferPoolManager,
    page::{Page, PAGE_SIZE},
};
use async_channel::{Receiver, Sender};
use std::io::Result;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::Mutex;
use tokio_uring::buf::{IoBuf, IoBufMut};

/// The number of frames in a [`FrameGroup`].
pub const FRAME_GROUP_SIZE: usize = 64;

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub struct Frame {
    /// The unique ID of this `Frame`.
    ///
    /// TODO more docs
    frame_id: usize,

    /// TODO docs
    page_owner: Option<Arc<Page>>,

    /// The buffer that this `Frame` holds ownership over.
    ///
    /// Since `Frame` is not [`Clone`]able, this `Frame` is guaranteed to have exclusive access to
    /// the mutable buffer.
    buf: &'static mut [u8],
}

/// A fixed group of frames.
#[derive(Debug)]
pub(crate) struct FrameGroup {
    /// The states of the [`Frame`]s that belong to this `FrameGroup`.
    ///
    /// Only 1 thread is allowed to modify eviction states at any time, thus we protect them with an
    /// asynchronous [`Mutex`].
    eviction_states: Mutex<[EvictionState; FRAME_GROUP_SIZE]>,

    /// An asynchronous channel of free [`Frame`]s.
    free_frames: (Sender<Frame>, Receiver<Frame>),
}

/// The enum representing the possible states that a [`Frame`] can be in with respect to the eviction
/// algorithm.
///
/// Note that these states may not necessarily be synced to the actual state of the [`Frame`]s, and
/// these only serve as hints to the eviction algorithm.
#[derive(Debug, Clone)]
pub(crate) enum EvictionState {
    /// Represents a frequently / recently accessed [`Frame`](super::frame::Frame) that currently
    /// holds a [`Page`](crate::page::Page)'s data.
    Hot(Arc<Page>),
    /// Represents an infrequently or old [`Frame`](super::frame::Frame) that might be evicted soon,
    /// and also still currently holds a [`Page`](crate::page::Page)'s data.
    Cool(Arc<Page>),
    /// Represents either a [`Frame`](super::frame::Frame) that does not hold any
    /// [`Page`](crate::page::Page)'s data, or a [`Frame`] that has an active thread trying to evict
    /// it from memory.
    Cold,
}

impl Default for EvictionState {
    fn default() -> Self {
        Self::Cold
    }
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

    /// Gets a pointer to the [`Page`] that owns this `Frame`, if this `Frame` is owned by any.
    pub fn get_page_owner(&self) -> Option<&Arc<Page>> {
        self.page_owner.as_ref()
    }

    /// Replaces the owning [`Page`] of this `Frame` with another [`Page`].
    pub fn replace_page_owner(&mut self, page: Arc<Page>) -> Option<Arc<Page>> {
        self.page_owner.replace(page)
    }

    /// Replaces the owning [`Page`] of this `Frame` with `None`.
    pub fn evict_page_owner(&mut self) -> Option<Arc<Page>> {
        self.page_owner.take()
    }

    /// Updates the eviction state after this frame has been accessed.
    pub async fn record_access(&self) {
        let group = self.group();
        let index = self.frame_id % FRAME_GROUP_SIZE;

        let mut guard = group.eviction_states.lock().await;
        match &mut guard[index] {
            EvictionState::Hot(_) => (),
            EvictionState::Cool(page) => guard[index] = EvictionState::Hot(page.clone()),
            EvictionState::Cold => (),
        }
    }
}

impl FrameGroup {
    /// Creates a new [`FrameGroup`] given an iterator of [`FRAME_GROUP_SIZE`] frames.
    ///
    /// # Panics
    ///
    /// This function will panic if the iterator does not contain exactly [`FRAME_GROUP_SIZE`]
    /// frames.
    pub async fn new<I>(frames: I) -> Self
    where
        I: IntoIterator<Item = Frame>,
    {
        let (rx, tx) = async_channel::bounded(FRAME_GROUP_SIZE);

        let mut counter = 0;
        for frame in frames {
            rx.send(frame).await.expect("Channel cannot be closed");
            counter += 1;
        }
        assert_eq!(counter, FRAME_GROUP_SIZE);

        let eviction_states = core::array::from_fn(|_| EvictionState::default());

        Self {
            eviction_states: Mutex::new(eviction_states),
            free_frames: (rx, tx),
        }
    }

    /// Gets a free frame in this `FrameGroup`.
    ///
    /// This function will evict other frames in this `FrameGroup` if there are no free frames
    /// available.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub async fn get_free_frame(&self) -> Result<Frame> {
        loop {
            if let Ok(frame) = self.free_frames.1.try_recv() {
                return Ok(frame);
            }

            self.cool_frames().await?;
        }
    }

    /// Runs the second chance / clock algorithm on all of the [`Frame`]s in this `FrameGroup`, and
    /// then evicts all of the frames that have been cooled twice.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub async fn cool_frames(&self) -> Result<()> {
        let mut eviction_pages: Vec<Arc<Page>> = Vec::with_capacity(FRAME_GROUP_SIZE);

        let mut guard = self.eviction_states.lock().await;

        for frame_temperature in guard.iter_mut() {
            if let Some(page) = frame_temperature.cool() {
                eviction_pages.push(page);
            }
        }

        drop(guard);

        if eviction_pages.is_empty() {
            return Ok(());
        }

        let sm = StorageManager::get().create_handle().await?;

        // Attempt to evict all of the already cool frames.
        for page in eviction_pages {
            // If we cannot get the write guard immediately, then someone else has it and we don't
            // need to evict this frame now.
            if let Ok(mut guard) = page.frame.try_write() {
                // Someone might have gotten in front of us and already evicted this page
                if guard.is_some() {
                    // Take ownership over the frame and remove from the Page
                    let mut frame = guard.take().unwrap();
                    frame
                        .evict_page_owner()
                        .expect("Tried to evict a frame that had no page owner");

                    // Write the data out to persistent storage
                    let (res, frame) = sm.write_from(page.pid, frame).await;
                    res?;

                    self.free_frames.0.send(frame).await.unwrap();
                }
            }
        }

        Ok(())
    }
}

impl EvictionState {
    /// Runs the cooling algorithm, returning a [`PageRef`] if we want to evict the page.
    ///
    /// If the state is [`Hot`](FrameTemperature::Hot), then this function cools it down to be
    /// [`Cool`](FrameTemperature::Cool), and if it was already [`Cool`](FrameTemperature::Cool),
    /// then this function does nothing. It is on the caller to deal with eviction of the
    /// [`Cool`](FrameTemperature::Cool) page via the [`PageRef`] that is returned.
    ///
    /// If the state transitions to [`Cold`](FrameTemperature::Cold), this function will return the
    /// [`PageRef`] that it used to hold.
    pub(crate) fn cool(&mut self) -> Option<Arc<Page>> {
        match self {
            Self::Hot(page) => {
                *self = Self::Cool(page.clone());
                None
            }
            Self::Cool(page) => Some(page.clone()),
            Self::Cold => None,
        }
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
