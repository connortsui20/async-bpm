use crate::{
    frame::Frame,
    io::IoUringAsync,
    page::{page_handle::PageHandle, PageId, PageRef, PAGE_SIZE},
};
use crossbeam_queue::ArrayQueue;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, RwLock};

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    pub(crate) active_pages: Mutex<Vec<PageRef>>,
    pub(crate) free_frames: ArrayQueue<Frame>,
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,
    num_frames: usize,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager.
    pub fn new(num_frames: usize) -> Self {
        // All frames start out as free
        let free_frames = ArrayQueue::new(num_frames);

        // Allocate all of the memory up front
        let allocation = vec![0u8; num_frames * PAGE_SIZE];

        let bytes: &'static mut [u8] = allocation.leak();
        debug_assert_eq!(bytes.len(), num_frames * PAGE_SIZE);

        // Note: use `as_chunks_unchecked_mut()` when it is stabilized
        // https://doc.rust-lang.org/std/primitive.slice.html#method.as_chunks_unchecked_mut
        for buf in bytes.chunks_exact_mut(PAGE_SIZE) {
            let frame = Frame::new(buf);
            free_frames
                .push(frame)
                .expect("Was not able to add the frame to the free_frames list");
        }

        Self {
            active_pages: Mutex::new(Vec::with_capacity(num_frames)),
            free_frames,
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
            num_frames,
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    // Constructs a thread-local handle to a logical page
    pub async fn get_page(self: Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;
        let page = pages_guard.get(&pid)?.clone();

        let uring = IoUringAsync::try_default().ok()?;

        Some(PageHandle { page, uring })
    }
}
