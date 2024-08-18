//! This module contains the declaration and implementation of the [`BufferPoolManager`] type.
//!
//! TODO docs.

use crate::{
    page::{Page, PageHandle, PageId, PAGE_SIZE},
    storage::{Frame, FrameGroup, StorageManager, FRAME_GROUP_SIZE},
};
use rand::prelude::*;
use scc::hash_map::HashMap;
use std::sync::{atomic::AtomicBool, Arc, OnceLock};
use std::{future::Future, io::Result};
use tokio::sync::RwLock;
use tokio::task;

/// The global buffer pool manager instance.
static BPM: OnceLock<BufferPoolManager> = OnceLock::new();

/// A parallel Buffer Pool Manager that manages bringing logical pages from persistent storage into
/// memory via shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this [`BufferPoolManager`] manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`Page`]s.
    ///
    /// Note that this is _not_ the same as a page table in a traditional buffer pool manager. In a
    /// traditional buffer pool manager, _every_ single lookup to a page must go through a global
    /// hash table. This hash table is different, in that a task is expected to get a page handle
    /// _once_ from the buffer pool, and then use that page handle to access the underlying page
    /// instead.
    ///
    /// TODO it is not strictly necessary that we need to store the `Arc<Page>` inside the hash
    /// table - the user should be allowed to manage the pages themselves (for example, if they are
    /// performing a scan we don't want to saturate this hash table with temporary pages).
    pages: HashMap<PageId, Arc<Page>>,

    /// All of the [`FrameGroup`]s that hold the [`Frame`]s that this buffer pool manages.
    frame_groups: Vec<Arc<FrameGroup>>,
}

/// TODO add method that creates a page but does not add it to the global page table.
impl BufferPoolManager {
    /// Constructs a new buffer pool manager with the given number of [`PAGE_SIZE`]ed buffer frames
    /// and an initial file capacity for storage.
    ///
    /// The amount of memory the buffer pool will manage is determined by `num_frames`, and the
    /// amount of data stored in persistent storage (for example, a hard drive) is determined by
    /// `capacity`.
    ///
    /// Note that this function may round `num_frames` down to a multiple of `FRAME_GROUP_SIZE`,
    /// which is an internal constant that groups memory frames together. Expect this constant to be
    /// set to 64 frames, but _do not_ rely on this fact.
    ///
    /// # Panics
    ///
    /// This function will panic if `num_frames` is equal to zero, if `capacity` is greater than
    /// or equal to `num_frames`, or if the caller has already called `initialize` before.
    pub fn initialize(num_frames: usize, capacity: usize) {
        assert!(
            BPM.get().is_none(),
            "Tried to initialize a BufferPoolManager more than once"
        );

        // Round down to the nearest multiple of `FRAME_GROUP_SIZE`.
        let num_frames = num_frames - (num_frames % FRAME_GROUP_SIZE);

        assert!(num_frames != 0);
        assert!(num_frames < capacity);

        let num_groups = num_frames / FRAME_GROUP_SIZE;

        // Allocate all of the buffer memory up front and initialize to 0s.
        let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();

        // Divide the memory up into `PAGE_SIZE` chunks.
        let buffers: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
        debug_assert_eq!(buffers.len(), num_frames);

        let mut frames: Vec<Frame> = buffers
            .into_iter()
            .enumerate()
            .map(|(i, buf)| Frame::new(i, buf))
            .collect();

        let mut frame_groups: Vec<Arc<FrameGroup>> = Vec::with_capacity(num_groups);

        for id in 0..num_groups {
            let group: Vec<Frame> = (0..FRAME_GROUP_SIZE)
                .map(|_| frames.pop().expect("Somehow ran out of frames"))
                .collect();
            frame_groups.push(Arc::new(FrameGroup::new(id, group)));
        }

        // Create the buffer pool and set it as the global static instance.
        BPM.set(Self {
            num_frames,
            pages: HashMap::with_capacity(num_frames),
            frame_groups,
        })
        .expect("Tried to initialize the buffer pool manager more than once");

        // Also initialize the global `StorageManager` instance.
        StorageManager::initialize(capacity);
    }

    /// Retrieve a static reference to the global buffer pool manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before [`BufferPoolManager::initialize`] has been
    /// called.
    pub fn get() -> &'static Self {
        BPM.get()
            .expect("Tried to get a reference to the BPM before it was initialized")
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Gets a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page does not already exist, this function will create it and then return it.
    ///
    /// # Errors
    ///
    /// If this function is unable to create a [`File`](tokio_uring::fs::File), this function will
    /// raise the I/O error in the form of [`Result`].
    pub fn get_page(&self, pid: &PageId) -> Result<PageHandle> {
        let sm: crate::storage::StorageManagerHandle = StorageManager::get().create_handle()?;

        // Get the page if it exists, otherwise create a new one return that.
        let page = self
            .pages
            .entry(*pid)
            .or_insert_with(|| {
                Arc::new(Page {
                    pid: *pid,
                    is_loaded: AtomicBool::new(false),
                    frame: RwLock::new(None),
                })
            })
            .get()
            .clone();

        Ok(PageHandle::new(page, sm))
    }

    /// Gets an [`Arc`] to a [`FrameGroup`] given the frame group ID.
    pub(crate) fn get_frame_group(&self, group_id: usize) -> Arc<FrameGroup> {
        self.frame_groups[group_id].clone()
    }

    /// Gets an [`Arc`] to a random [`FrameGroup`] in the buffer pool manager.
    ///
    /// Intended for use by an eviction algorithm.
    pub(crate) fn get_random_frame_group(&self) -> Arc<FrameGroup> {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.frame_groups.len());

        self.get_frame_group(index)
    }

    /// Starts a [`tokio_uring`] runtime on a single thread that runs the given [`Future`].
    ///
    /// TODO more docs
    ///
    /// # Panics
    ///
    /// This function will panic if it is unable to spawn the eviction task for some reason.
    pub fn start_thread<F: Future>(future: F) -> F::Output {
        tokio_uring::start(async move {
            tokio::select! {
                output = future => output,
                // TODO figure out why including this is this slower
                _ = Self::spawn_evictor() => unreachable!("The eviction task should never return")
            }
        })
    }

    /// Spawns a thread-local task on the current thread.
    ///
    /// Note that the caller must `.await` the return of this function in order to run the future.
    ///
    /// TODO docs
    pub fn spawn_local<T: Future + 'static>(task: T) -> task::JoinHandle<T::Output> {
        tokio_uring::spawn(task)
    }

    /// Spawns an eviction task.
    ///
    /// TODO more docs
    ///
    /// # Panics
    ///
    /// Panics if unable to evict frames due to an I/O error.
    pub fn spawn_evictor() -> task::JoinHandle<()> {
        tokio_uring::spawn(async {
            let bpm = Self::get();
            loop {
                tokio::task::yield_now().await;

                let group = bpm.get_random_frame_group();
                if group.num_free_frames() < FRAME_GROUP_SIZE / 10 {
                    group
                        .cool_frames()
                        .await
                        .expect("Unable to evict frames due to I/O error");
                }

                // Sleep once we have nothing to do.
                // TODO removing this should not cause the system to halt.
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        })
    }
}
