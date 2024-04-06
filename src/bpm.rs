use crate::{
    disk::{disk_manager::DiskManager, frame::Frame},
    page::{
        eviction::{Temperature, TemperatureState},
        Page, PageHandle, PageId, PageRef, PAGE_SIZE,
    },
};
use async_channel::{Receiver, Sender};
use futures::future;
use std::{
    collections::{HashMap, HashSet},
    io::IoSlice,
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::{Mutex, RwLock};

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
    pub fn new(num_initial_pages: usize, num_frames: usize) -> Self {
        // All frames start out as free
        let (tx, rx) = async_channel::bounded(num_frames);

        // Allocate all buffer memory up front and leak it so it never gets dropped
        let io_slices = {
            let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();

            // Note: use `as_chunks_unchecked_mut()` when it is stabilized:
            // https://doc.rust-lang.org/std/primitive.slice.html#method.as_chunks_unchecked_mut
            let slices: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
            let buffers: Vec<IoSlice<'static>> =
                slices.into_iter().map(|buf| IoSlice::new(buf)).collect();

            // Create owned versions of each buffer and place them into th
            for buf in buffers.iter().copied() {
                let frame = Frame::new(buf);
                tx.send_blocking(frame)
                    .expect("Was unable to send the initial frames onto the global free list");
            }

            buffers.leak()
        };

        Self {
            num_frames,
            active_pages: Mutex::new(HashSet::with_capacity(num_frames)),
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
            free_frames: (tx, rx),
            disk_manager: Arc::new(DiskManager::new(
                num_initial_pages,
                "db.test".to_string(),
                io_slices,
            )),
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Creates a logical page in the buffer pool manager, returning `PageHandle` to the page.
    ///
    /// If the page already exists, returns `None`.
    pub async fn create_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let mut pages_guard = self.pages.write().await;

        if pages_guard.contains_key(&pid) {
            return None;
        }

        let page = Arc::new(Page {
            pid,
            eviction_state: Temperature::new(TemperatureState::Cold),
            inner: RwLock::new(None),
        });

        pages_guard.insert(pid, page.clone());

        let disk_manager_handle = self.disk_manager.create_handle();

        Some(PageHandle {
            page,
            bpm: self.clone(),
            dm: disk_manager_handle,
        })
    }

    /// Constructs a thread-local handle to a logical page, as long as the page already exists.
    /// If it does not exist in the context of the buffer pool manager, returns `None`.
    ///
    /// If the page does not already exist, use `create_page` instead to get a `PageHandle`.
    pub async fn get_page(self: &Arc<Self>, pid: &PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;

        let page = pages_guard.get(pid)?.clone();

        let disk_manager_handle = self.disk_manager.create_handle();

        Some(PageHandle {
            page,
            bpm: self.clone(),
            dm: disk_manager_handle,
        })
    }

    /// Cools a given page, evicting it if it is already cool.
    async fn cool(self: &Arc<Self>, pid: &PageId) {
        let page_handle = self
            .get_page(pid)
            .await
            .expect("Could not find an active page in the map of pages");

        match page_handle.page.eviction_state.load(Ordering::Acquire) {
            TemperatureState::Cold => panic!("Found a Cold page in the active list of pages"),
            TemperatureState::Cool => page_handle.evict().await,
            TemperatureState::Hot => page_handle
                .page
                .eviction_state
                .store(TemperatureState::Cool, Ordering::Release),
        }
    }

    /// Continuously runs the eviction algorithm in a loop.
    ///
    /// This `Future` should be placed onto the task queue of every worker so that it does not stall
    /// waiting for other worker threads to manually release buffers.
    pub async fn evictor(self: &Arc<Self>) -> ! {
        let bpm = self.clone();

        loop {
            // Pages referenced in the `active_pages` list are guaranteed to own `Frame`s
            let active_guard = bpm.active_pages.lock().await;

            // Run all eviction futures concurrently
            let futures = active_guard.deref().iter().map(|pid| self.cool(pid));
            future::join_all(futures).await;
        }
    }
}
