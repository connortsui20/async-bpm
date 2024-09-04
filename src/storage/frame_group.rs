//! This module contains the type definiton and implementation for the [`FrameGroup`] struct.
//!
//! TODO more docs.

use crate::page::Page;
use crate::storage::frame::Frame;
use crate::storage::storage_manager::StorageManager;
use std::io::Result;
// use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

/// The number of frames in a [`FrameGroup`].
pub(crate) const FRAME_GROUP_SIZE: usize = 64;

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
    pub(crate) group_id: usize,

    /// The states of the [`Frame`]s that belong to this `FrameGroup`.
    ///
    /// Note that we use a blocking mutex here because we do not need to hold the lock across any
    /// `.await` points.
    pub(crate) eviction_states: Mutex<[EvictionState; FRAME_GROUP_SIZE]>,

    /// The number of free frames in the free list.
    pub(crate) num_free_frames: AtomicUsize,

    /// An hronous channel of free [`Frame`]s. Behaves as the free list of frames.
    pub(crate) free_list: (Sender<Frame>, Receiver<Frame>),
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
        let (tx, rx) = bounded::<Frame>(FRAME_GROUP_SIZE);

        let mut counter = 0;
        for frame in frames {
            tx.send(frame).expect("Channel cannot be closed");
            counter += 1;
        }
        assert_eq!(counter, FRAME_GROUP_SIZE);

        let eviction_states = core::array::from_fn(|_| EvictionState::default());

        Self {
            group_id,
            eviction_states: Mutex::new(eviction_states),
            num_free_frames: AtomicUsize::new(FRAME_GROUP_SIZE),
            free_list: (tx, rx),
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
    pub(crate) fn get_free_frame(&self) -> Result<Frame> {
        loop {
            if let Ok(frame) = self.free_list.1.try_recv() {
                self.num_free_frames.fetch_sub(1, Ordering::Release);
                return Ok(frame);
            }
            self.cool_frames()?;
        }
    }

    /// Runs the second chance / clock algorithm on all of the [`Frame`]s in this `FrameGroup`, and
    /// then evicts all of the frames that have been cooled twice.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub(crate) fn cool_frames(&self) -> Result<()> {
        let mut eviction_pages: Vec<Arc<Page>> = Vec::with_capacity(FRAME_GROUP_SIZE);

        // Find page eviction candidates.
        {
            let mut evicton_guard = self
                .eviction_states
                .lock()
                .expect("Fatal: `EvictionState` lock was poisoned somehow");

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

        let sm = StorageManager::get().create_handle()?;

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
                let frame = sm.write_from(page.pid, frame)?;

                self.free_list.0.send(frame).unwrap();
                self.num_free_frames.fetch_add(1, Ordering::Release);
            }
        }

        Ok(())
    }
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
