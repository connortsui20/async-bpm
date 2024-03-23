use std::{collections::HashMap, sync::Arc};

use crate::{
    frame::Frame,
    page::{page_handle::PageHandle, Page, PageId},
};
use crossbeam_queue::ArrayQueue;
use tokio::sync::RwLock;

pub struct BufferPoolManager {
    pub(crate) frames: Vec<Frame>,
    pub(crate) free_frames: ArrayQueue<Frame>,
    pub(crate) pages: RwLock<HashMap<PageId, Arc<Page>>>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager
    pub fn new(frame_num: usize) -> Self {
        // TODO create proper IoSlice frames
        Self {
            frames: (0..frame_num).map(|_| Frame::default()).collect(),
            free_frames: ArrayQueue::new(frame_num),
            pages: RwLock::new(HashMap::with_capacity(frame_num * 2)),
        }
    }

    // Constructs a thread-local handle to a logical page
    pub async fn get_page(self: Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;
        let page = pages_guard.get(&pid)?.clone();

        Some(PageHandle { page })
    }
}
