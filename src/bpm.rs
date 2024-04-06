use crate::{
    disk::{disk_manager::DiskManager, frame::Frame},
    page::{
        eviction::{Temperature, TemperatureState},
        Page, PageHandle, PageId, PageRef, PAGE_SIZE,
    },
};
use async_channel::{Receiver, Sender};
use std::{collections::HashMap, io::IoSlice, sync::Arc};
use tokio::sync::RwLock;

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this Buffer Pool Manager manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,

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

        let io_slices = {
            // Allocate all of the memory up front and leak
            let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();
            println!(
                "Size of leaked buffers: {:#010X}",
                std::mem::size_of_val(bytes)
            );
            debug_assert_eq!(bytes.len(), num_frames * PAGE_SIZE);

            // Note: use `as_chunks_unchecked_mut()` when it is stabilized
            // https://doc.rust-lang.org/std/primitive.slice.html#method.as_chunks_unchecked_mut
            let slices: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
            debug_assert_eq!(slices.len(), num_frames);

            let buffers: Vec<IoSlice<'static>> =
                slices.into_iter().map(|buf| IoSlice::new(buf)).collect();

            for buf in buffers.iter().copied() {
                let frame = Frame::new(buf);
                tx.send_blocking(frame)
                    .expect("Was unable to send the initial frames onto the global free list");
            }

            let io_slices: &'static [IoSlice] = buffers.leak();
            assert_eq!(io_slices.len(), num_frames);
            println!(
                "Size of leaked vector of IoSlice: {:#010X}",
                std::mem::size_of_val(io_slices)
            );

            io_slices
        };

        Self {
            num_frames,
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
            bpm: self.clone(),
        });

        pages_guard.insert(pid, page.clone());

        let disk_manager_handle = self.disk_manager.create_handle();

        Some(PageHandle {
            page,
            dm: disk_manager_handle,
        })
    }

    /// Constructs a thread-local handle to a logical page, as long as the page already exists.
    /// If it does not exist in the context of the buffer pool manager, returns `None`.
    ///
    /// If the page does not already exist, use `create_page` instead to get a `PageHandle`.
    pub async fn get_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;

        let page = pages_guard.get(&pid)?.clone();

        let disk_manager_handle = self.disk_manager.create_handle();

        Some(PageHandle {
            page,
            dm: disk_manager_handle,
        })
    }
}
