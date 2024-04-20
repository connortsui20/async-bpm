use crate::{
    disk::{
        disk_manager::{DiskManager, DiskManagerHandle},
        frame::Frame,
    },
    page::{
        eviction::{Temperature, TemperatureState},
        Page, PageHandle, PageId, PageRef, PAGE_SIZE,
    },
};
use async_channel::{Receiver, Sender};
use core::slice;
use futures::future;
use std::{
    collections::{HashMap, HashSet},
    io::IoSlice,
    io::IoSliceMut,
    ops::Deref,
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this [`BufferPoolManager`] manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,

    /// A collection of [`PageId`]s that currently have their data in memory.
    ///
    /// Used to help find pages to evict from memory. The [`BufferPoolManager`] must maintain this
    /// [`HashSet`] by ensuring it is synced with the pages that are brought into memory.
    pub(crate) active_pages: Mutex<HashSet<PageId>>,

    /// A channel of free, owned buffer [`Frame`]s.
    pub(crate) free_frames: (Sender<Frame>, Receiver<Frame>),

    /// The manager of reading and writing [`Page`] data via [`Frame`]s.
    pub(crate) disk_manager: Arc<DiskManager>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager with the given number of `PAGE_SIZE`ed buffer frames.
    ///
    /// The argument `capacity` should be the starting number of logical pages the user of the
    /// [`BufferPoolManager`] wishes to use, as it will allocate enough space on disk to initially
    /// accommodate that number.
    pub fn new(num_frames: usize, capacity: usize) -> Self {
        // All frames start out as free
        let (tx, rx) = async_channel::bounded(num_frames);

        // Allocate all of the buffer memory up front
        let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();

        // Note: should use `as_chunks_unchecked_mut()` instead once it is stabilized:
        // https://doc.rust-lang.org/std/primitive.slice.html#method.as_chunks_unchecked_mut
        let slices: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
        assert_eq!(slices.len(), num_frames);

        // Create the registerable buffers, as well as create the owned `Frame`s and send them down
        // the channel for future `Page`s to take ownership of
        let register_buffers = slices
            .into_iter()
            .map(|buf| {
                // Safety: Since we never actually read from the buffer pointers (intended for being
                // registered in an `io_uring` instance), it is safe to have a shared slice
                // reference exist at the same time as the exclusive mutable slice reference that is
                // being stored through the `IoSliceMut` and `Frame`.
                let register_slice = unsafe { slice::from_raw_parts(buf.as_ptr(), PAGE_SIZE) };

                {
                    // Create the owned `Frame`
                    let owned_buf = IoSliceMut::new(buf);
                    let frame = Frame::new(owned_buf);

                    // Add the `Frame` to the channel of free frames
                    tx.send_blocking(frame)
                        .expect("Was unable to send the initial frames onto the global free list");
                }

                IoSlice::new(register_slice)
            })
            .collect::<Vec<IoSlice<'static>>>()
            .into_boxed_slice();

        let disk_manager = Arc::new(DiskManager::new(
            capacity,
            "db.test".to_string(),
            register_buffers,
        ));

        Self {
            num_frames,
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
            active_pages: Mutex::new(HashSet::with_capacity(num_frames)),
            free_frames: (tx, rx),
            disk_manager,
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Creates a thread-local page handle of the buffer pool manager, returning a `PageHandle` to
    /// the logical page data.
    ///
    /// If the page already exists, this function will return that instead.
    async fn create_page(self: &Arc<Self>, pid: &PageId) -> PageHandle {
        info!("Creating {} Handle", pid);

        // First check if it exists already
        let mut pages_guard = self.pages.write().await;
        if let Some(page) = pages_guard.get(pid) {
            return PageHandle::new(
                page.clone(),
                self.clone(),
                self.disk_manager.create_handle(),
            );
        }

        // Create the new page and update the global map of pages
        let page = Arc::new(Page {
            pid: *pid,
            eviction_state: Temperature::new(TemperatureState::Cold),
            inner: RwLock::new(None),
            bpm: self.clone(),
        });

        pages_guard.insert(*pid, page.clone());

        // Create the page handle and return
        PageHandle::new(page, self.clone(), self.disk_manager.create_handle())
    }

    /// Gets a thread-local page handle of the buffer pool manager, returning a `PageHandle` to
    /// the logical page data.
    ///
    /// If the page does not already exist, this function will create it and then return it.
    pub async fn get_page(self: &Arc<Self>, pid: &PageId) -> PageHandle {
        debug!("Getting {} Handle", pid);

        let pages_guard = self.pages.read().await;

        // Get the page if it exists, otherwise create it and return
        let page = match pages_guard.get(pid) {
            Some(page) => page.clone(),
            None => {
                drop(pages_guard);
                return self.create_page(pid).await;
            }
        };

        PageHandle::new(page, self.clone(), self.disk_manager.create_handle())
    }

    // Creates a thread-local [`DiskManagerHandle`] to the inner [`DiskManager`].
    pub fn get_disk_manager(&self) -> DiskManagerHandle {
        self.disk_manager.create_handle()
    }

    /// Continuously runs the eviction algorithm in a loop.
    ///
    /// This `Future` should be placed onto the task queue of every worker so that it does not stall
    /// waiting for other worker threads to manually release buffers.
    pub async fn evictor(self: &Arc<Self>) -> ! {
        let bpm = self.clone();

        loop {
            // Pages referenced in the `active_pages` list are guaranteed to own `Frame`s
            let Ok(active_guard) = bpm.active_pages.try_lock() else {
                debug!("Contention on Active Pages in Evictor");
                continue;
            };

            let pids: Vec<_> = active_guard.deref().iter().copied().collect();

            drop(active_guard);

            if pids.is_empty() {
                continue;
            }

            info!("Active pages: {:?}", pids);

            // Run all eviction futures concurrently
            future::join_all(pids.iter().map(|pid| async {
                let ph = self.get_page(pid).await;
                ph.cool().await;
            }))
            .await;
        }
    }
}
