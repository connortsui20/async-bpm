//! This module contains the definition and implementation of [`Frame`] and [`FrameGroup`], which
//! are types that represent the buffer frames that the buffer pool manager is in charge of.
//!
//! A [`Frame`] is intended to hold [`PAGE_SIZE`] bytes of data, and is also intended to be shared
//! with the the kernel to avoid unnecessary `memcpy`s from the kernel's internal buffers into
//! user-space buffers.
//!
//! A [`FrameGroup`] instance groups [`Frame`]s together so that evictions do not have to search
//! every single [`Frame`] in the buffer pool for an eviction candidate.

use crate::storage::storage_manager::StorageManager;
use crate::{
    bpm::BufferPoolManager,
    page::{Page, PAGE_SIZE},
};
use async_channel::{Receiver, Sender};
use std::io::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio_uring::buf::{IoBuf, IoBufMut};

/// The number of frames in a [`FrameGroup`].
pub(crate) const FRAME_GROUP_SIZE: usize = 64;

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
    buf: &'static mut [u8],
}

/// A fixed group of frames.
///
/// The `FrameGroup` is a data structure intended to make finding evictions easier for the system.
/// Instead of requiring every eviction task to scan the entire buffer pool for an eviction
/// candidate, we group the frames together and randomly choose one `FrameGroup` from which we will
/// choose an eviction candidate from instead.
///
/// By grouping frames together as such, we can say that a [`Frame`] can be in one of three states:
/// - A [`Frame`] can be owned by a [`Page`]
///     - The [`Frame`]'s [`EvictionState`] can be either [`Hot`] or [`Cool`]
/// - A [`Frame`] can have an active task trying to evict the data the [`Frame`] holds
///     - The [`Frame`]'s [`EvictionState`] can be either [`Cool`] or [`Cold`]
/// - A [`Frame`] can be in the free list of frames in a `FrameGroup`
///     - The [`Frame`]'s [`EvictionState`] _must_ be [`Cold`]
///
/// [`Hot`]: EvictionState::Hot
/// [`Cool`]: EvictionState::Cool
/// [`Cold`]: EvictionState::Cold
#[derive(Debug)]
pub(crate) struct FrameGroup {
    /// The unique ID of this `FrameGroup`.
    #[allow(dead_code)]
    group_id: usize,

    /// The states of the [`Frame`]s that belong to this `FrameGroup`.
    ///
    /// Note that we use a blocking mutex here because we do not need to hold the lock across any
    /// `.await` points.
    eviction_states: Mutex<[EvictionState; FRAME_GROUP_SIZE]>,

    /// The number of free frames in the free list.
    num_free_frames: AtomicUsize,

    /// An asynchronous channel of free [`Frame`]s. Behaves as the free list of frames.
    free_list: (Sender<Frame>, Receiver<Frame>),
}

/// The enum representing the possible states that a [`Frame`] can be in with respect to the
/// eviction algorithm.
///
/// Note that these states may not necessarily be synced to the actual state of the [`Frame`]s, and
/// these only serve as hints to the eviction algorithm.
#[derive(Debug, Clone)]
pub(crate) enum EvictionState {
    /// Represents a frequently / recently accessed [`Frame`] that currently holds a [`Page`]'s
    /// data.
    Hot(Arc<Page>),
    /// Represents an infrequently or old [`Frame`] that might be evicted soon, and also still
    /// currently holds a [`Page`] data.
    Cool(Arc<Page>),
    /// Represents either a [`Frame`] that does not hold any [`Page`] data, or a [`Frame`] that has
    /// an active thread trying to evict it from memory.
    Cold,
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
            .expect("EvictionState lock was poisoned somehow");

        eviction_guard[index] = EvictionState::Hot(page.clone());
    }
}

impl FrameGroup {
    /// Creates a new [`FrameGroup`] given an iterator of [`FRAME_GROUP_SIZE`] frames.
    ///
    /// # Panics
    ///
    /// This function will panic if the iterator does not contain exactly [`FRAME_GROUP_SIZE`]
    /// frames.
    pub(crate) fn new<I>(group_id: usize, frames: I) -> Self
    where
        I: IntoIterator<Item = Frame>,
    {
        let (rx, tx) = async_channel::bounded(FRAME_GROUP_SIZE);

        let mut counter = 0;
        for frame in frames {
            rx.send_blocking(frame).expect("Channel cannot be closed");
            counter += 1;
        }
        assert_eq!(counter, FRAME_GROUP_SIZE);

        let eviction_states = core::array::from_fn(|_| EvictionState::default());

        Self {
            group_id,
            eviction_states: Mutex::new(eviction_states),
            num_free_frames: AtomicUsize::new(FRAME_GROUP_SIZE),
            free_list: (rx, tx),
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
    pub(crate) async fn get_free_frame(&self) -> Result<Frame> {
        loop {
            if let Ok(frame) = self.free_list.1.try_recv() {
                self.num_free_frames.fetch_sub(1, Ordering::Release);
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
    pub(crate) async fn cool_frames(&self) -> Result<()> {
        let mut eviction_pages: Vec<Arc<Page>> = Vec::with_capacity(FRAME_GROUP_SIZE);

        // Find page eviction candidates.
        {
            let mut evicton_guard = self
                .eviction_states
                .lock()
                .expect("EvictionState lock was poisoned somehow");

            for frame_temperature in evicton_guard.iter_mut() {
                if let Some(page) = frame_temperature.cool() {
                    eviction_pages.push(page);
                }
            }
        }

        // If there are no page eviction candidates, then there is nothing we can do.
        if eviction_pages.is_empty() {
            return Ok(());
        }

        let sm = StorageManager::get().create_handle().await?;

        // Attempt to evict all of the already cool frames.
        for page in eviction_pages {
            // If we cannot get the write guard immediately, then someone else has it and we don't
            // need to evict this frame now.
            if let Ok(mut guard) = page.frame.try_write() {
                // Check if someone got in front of us and already evicted this page.
                if guard.is_none() {
                    continue;
                }

                page.is_loaded.store(false, Ordering::Release);

                // Take ownership over the frame and remove from the page.
                let mut frame = guard.take().unwrap();
                frame
                    .evict_page_owner()
                    .expect("Tried to evict a frame that had no page owner");

                // Write the data out to persistent storage.
                let (res, frame) = sm.write_from(page.pid, frame).await;
                res?;

                self.free_list.0.send(frame).await.unwrap();
                self.num_free_frames.fetch_add(1, Ordering::Release);
            }
        }

        Ok(())
    }

    /// Gets the number of free frames in this `FrameGroup`.
    pub(crate) fn num_free_frames(&self) -> usize {
        self.num_free_frames.load(Ordering::Acquire)
    }
}

impl EvictionState {
    /// Runs the cooling algorithm, returning an optional [`Page`] if we want to evict the
    /// page.
    ///
    /// If the state is [`Hot`](EvictionState::Hot), then this function cools it down to be
    /// [`Cool`](EvictionState::Cool), and if it was already [`Cool`](EvictionState::Cool), then
    /// this function does nothing. It is on the caller to deal with eviction of the
    /// [`Cool`](EvictionState::Cool) page via the [`Page`] that is returned.
    ///
    /// If the state transitions to [`Cold`](EvictionState::Cold), this function will return the
    /// [`Page`] that it used to hold.
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

impl Default for EvictionState {
    fn default() -> Self {
        Self::Cold
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
