//! This module contains the declaration and implementation of the [`BufferPoolManager`] type.
//!
//! This buffer pool manager has an asynchronous implementation that is built on top of an
//! asynchronous persistent / non-volatile storage manager, which is itself built on top of the
//! Linux `io_uring` interface.
//!
//! The goal for this buffer pool manager is to exploit parallelism as much as possible by limiting
//! the use of any global latches or single points of contention for the entire system. This means
//! that several parts of the system are implemented quite differently from how a traditional buffer
//! pool manager would work.

use crate::{
    page::{Page, PageHandle, PageId},
    storage::{frame::FrameGroup, storage_manager::StorageManager},
};
use rand::prelude::*;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc, OnceLock},
};
use tokio::sync::RwLock;

/// The global buffer pool manager instance.
static BPM: OnceLock<BufferPoolManager> = OnceLock::new();

/// A parallel Buffer Pool Manager that manages bringing logical pages from persistent storage into
/// memory via shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this [`BufferPoolManager`] manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pages: RwLock<HashMap<PageId, Arc<Page>>>,

    /// TODO docs
    frame_groups: Vec<Arc<FrameGroup>>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager with the given number of [`PAGE_SIZE`]ed buffer frames.
    ///
    /// The argument `capacity` should be the starting number of logical pages the user of the
    /// [`BufferPoolManager`] wishes to use, as it will allocate enough space persistent storage to
    /// initially accommodate that number. TODO this is subject to change once the storage manager
    /// improves.
    ///
    /// This function will create two copies of the buffers allocated, 1 copy for user access
    /// through `Frame`s and `FrameGroup`s, and another copy for kernel access by registering the
    /// buffers into the `io_uring` instance via
    /// [`register_buffers`](io_uring::Submitter::register_buffers).
    ///
    /// # Panics
    ///
    /// This function will panic if `num_frames` is not a multiple of
    /// [`FRAME_GROUP_SIZE`]((crate::storage::frame::FRAME_GROUP_SIZE)).
    pub fn initialize(_num_frames: usize, _capacity: usize) {
        todo!()
    }

    /// Retrieve a static reference to the global buffer pool manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`BufferPoolManager::initialize`].
    pub fn get() -> &'static Self {
        BPM.get()
            .expect("Tried to get a reference to the BPM before it was initialized")
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Gets an `Arc` to a [`FrameGroup`] given the frame group ID.
    pub(crate) fn get_frame_group(&self, group_id: usize) -> Arc<FrameGroup> {
        self.frame_groups[group_id].clone()
    }

    /// Gets an `Arc` to a random [`FrameGroup`] in the buffer pool manager.
    ///
    /// Intended for use by an eviction algorithm.
    pub(crate) fn get_random_frame_group(&self) -> Arc<FrameGroup> {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.frame_groups.len());

        self.get_frame_group(index)
    }

    /// Creates a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page already exists, this function will return that instead.
    async fn create_page(&self, pid: &PageId) -> PageHandle {
        // First check if it exists already
        let mut pages_guard = self.pages.write().await;
        if let Some(page) = pages_guard.get(pid) {
            return PageHandle::new(
                page.clone(),
                StorageManager::get().create_handle().await.expect("TODO"),
            );
        }

        // Create the new page and update the global map of pages
        let page = Arc::new(Page {
            pid: *pid,
            is_loaded: AtomicBool::new(false),
            frame: RwLock::new(None),
        });

        pages_guard.insert(*pid, page.clone());

        // Create the page handle and return
        PageHandle::new(
            page,
            StorageManager::get().create_handle().await.expect("TODO"),
        )
    }

    /// Gets a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page does not already exist, this function will create it and then return it.
    pub async fn get_page(&self, pid: &PageId) -> PageHandle {
        let pages_guard = self.pages.read().await;

        // Get the page if it exists, otherwise create it and return
        let page = match pages_guard.get(pid) {
            Some(page) => page.clone(),
            None => {
                drop(pages_guard);
                return self.create_page(pid).await;
            }
        };

        PageHandle::new(
            page,
            StorageManager::get().create_handle().await.expect("TODO"),
        )
    }
}
