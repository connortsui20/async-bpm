use std::{collections::HashMap, sync::Arc};
use crate::{
    frame::Frame,
    io::IoUringAsync,
    page::{page_handle::PageHandle, Page, PageId},
};
use crossbeam_queue::ArrayQueue;
use tokio::sync::RwLock;

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
pub struct BufferPoolManager {
    pub(crate) frames: Vec<Frame>,
    pub(crate) free_frames: ArrayQueue<Frame>,
    pub(crate) pages: RwLock<HashMap<PageId, Arc<Page>>>,
    num_frames: usize,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager.
    pub fn new(num_frames: usize) -> Self {
         // TODO register frames via IoSlice
        Self {
            frames: (0..num_frames).map(|_| Frame::default()).collect(),
            free_frames: ArrayQueue::new(num_frames),
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
